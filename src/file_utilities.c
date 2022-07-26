#include "file_utilities.h"
#include "buffer_manager.h"
#include "data_page.h"
#include "header_page.h"
#include <assert.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

// Local definition

static void lock_page(int file, uint64_t page_id, enum FileErrorStatus *error,
                      char *error_message, short l_type);

static void assert_page_size(uint64_t page_id);

// API implementation

int create_database_file(char *path, uint64_t no_elements,
                         enum FileErrorStatus *error) {
  assert(no_elements < MAX_NO_ELEMENTS);
  int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL, 0600);

  *error = success;
  if (-1 == fd) {
    fprintf(stderr, "cannot create database file, check permissions or if file "
                    "already exists.\n");
    return -1;
  }

  uint64_t no_pages_needed =
      (((no_elements * RECORD_SIZE_ESTIMATE - PAGE_SIZE + 1) / PAGE_SIZE)
       << 1) +
      1;
  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    return -1;
  }

  for (uint64_t i = 0; i < no_pages_needed; ++i) {
    if (0 == i) {
      write_lock_page(fd, i, error);
      if (failure == *error) {
        free_page_buffer(safe_buffer);
        return -1;
      }
      HeaderPage header_page = create_header_page(safe_buffer, no_pages_needed);
      write_page_to_file(fd, safe_buffer, i, true, error);
      if (failure == *error) {
        free_page_buffer(safe_buffer);
        return -1;
      }
    } else {
      DataPage data_page = data_page_from_data(safe_buffer, i);
      write_page_to_file(fd, safe_buffer, i, true, error);
      if (failure == *error) {
        free_page_buffer(safe_buffer);
        return -1;
      }
    }
  }
  unlock_page(fd, 0, error);
  if (failure == *error) {
    free_page_buffer(safe_buffer);
    return -1;
  }
  free_page_buffer(safe_buffer);
  return fd;
}

int open_database_file(char *path, enum FileErrorStatus *error) {
  int fd = open(path, O_RDWR);
  *error = success;

  if (-1 == fd) {
    fprintf(stderr, "cannot open database file, check permissions or if file "
                    "already exists.\n");
    *error = failure;
    return -1;
  }
  read_lock_page(fd, 0, error);
  if (failure == *error) {
    return -1;
  }
  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    return -1;
  }
  // check if header is correct
  read_page_into_buffer(fd, 0, safe_buffer, error);
  if (failure == *error) {
    free_page_buffer(safe_buffer);
    return -1;
  }
  HeaderPage header_page = open_header_page(safe_buffer);
  uint64_t local_header_version = header_version(&header_page);
  if (local_header_version != DATABASE_VERSION) {
    *error = failure;
    free_page_buffer(safe_buffer);
    return -1;
  }

  return fd;
}

void read_page_into_buffer(int file, uint64_t page_id, SafeBuffer *safe_buffer,
                           enum FileErrorStatus *error) {
  assert_page_size(page_id);

  *error = success;

  uint64_t offset_from_start = (page_id * PAGE_SIZE);
  off_t lseek_offset = lseek(file, offset_from_start, SEEK_SET);
  if (-1 == lseek_offset) {
    *error = failure;
    fprintf(stderr, "lseek failed while reading.");
    return;
  }

  ssize_t bytes_read = read(file, get_buffer(safe_buffer), PAGE_SIZE);
  if (PAGE_SIZE != bytes_read) {
    *error = failure;
    fprintf(stderr, "failed to read a page from file.");
    return;
  }

  set_buffer_length(safe_buffer, PAGE_SIZE);
}

void write_page_to_file(int file, SafeBuffer *safe_buffer, uint64_t page_id,
                        bool append_only, enum FileErrorStatus *error) {
  assert_page_size(page_id);

  *error = success;

  if (!append_only) {
    uint64_t offset_from_start = (page_id * PAGE_SIZE);
    off_t lseek_offset = lseek(file, offset_from_start, SEEK_SET);
    if (-1 == lseek_offset) {
      *error = failure;
      fprintf(stderr, "lseek failed while writing.");
      return;
    }
  }

  size_t bytes_written = write(file, get_buffer(safe_buffer), PAGE_SIZE);
  if (bytes_written != get_buffer_length(safe_buffer)) {
    fprintf(stderr, "failed to write to file.");
    *error = failure;
    return;
  }
  *error = success;
}

void read_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error) {
  assert_page_size(page_id);

  lock_page(file, page_id, error,
            "process interrupted while read locking page.", F_RDLCK);
}

void write_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error) {
  assert_page_size(page_id);

  lock_page(file, page_id, error,
            "process interrupted while write locking page.", F_WRLCK);
}

void unlock_page(int file, uint64_t page_id, enum FileErrorStatus *error) {
  assert_page_size(page_id);

  lock_page(file, page_id, error,
            "process interrupted while write locking page.", F_UNLCK);
}

void close_database_file(int fd, enum FileErrorStatus *error) {
  *error = success;
  unlock_page(fd, 0, error);
  if (-1 == fd) {
    *error = failure;
    return;
  }
  int close_error = close(fd);
  if (-1 == close_error) {
    *error = failure;
    fprintf(stderr, "failed to close database file.\n");
  }
  *error = success;
}

// Local implementation

static void lock_page(int file, uint64_t page_id, enum FileErrorStatus *error,
                      char *error_message, short l_type) {
  assert_page_size(page_id);

  *error = success;
  uint64_t offset_from_start = (page_id * PAGE_SIZE);

  struct flock lock = {
      .l_type = l_type,
      .l_whence = SEEK_SET,
      .l_start = offset_from_start,
      .l_len = PAGE_SIZE,
  };

  int fcntl_error = fcntl(file, F_SETLKW, &lock);
  if (-1 == fcntl_error) {
    *error = failure;
    fprintf(stderr, "%s", error_message);
  }
}

static void assert_page_size(uint64_t page_id) {
  assert(0 == (page_id >> (64 - PAGE_NO_BITS)));
}
