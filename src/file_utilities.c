#include "file_utilities.h"
#include "buffer_manager.h"
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

int create_database_file(char *path, uint64_t no_elements) {
  assert(no_elements < MAX_NO_ELEMENTS);
  int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_EXCL);
  if (-1 == fd) {
    fprintf(stderr, "cannot create database file, check permissions or if file "
                    "already exists.");
  }
  uint64_t no_pages_needed =
      ((no_elements * RECORD_SIZE_ESTIMATE - PAGE_HEADER_SIZE_ESTIMATE + 1) /
       PAGE_HEADER_SIZE_ESTIMATE) +
      1;
  for (uint64_t i = 0; i < no_pages_needed; ++i) {
    // write the pages one by one
  }
  return fd;
}

void read_page_into_buffer(int file, uint64_t page_id, SafeBuffer *safe_buffer,
                           enum FileErrorStatus *error) {
  assert_page_size(page_id);

  *error = failure;

  uint64_t offset_from_start = (page_id * PAGE_SIZE);
  off_t lseek_offset = lseek(file, offset_from_start, SEEK_SET);
  if (-1 == lseek_offset) {
    fprintf(stderr, "lseek failed while reading.");
    return;
  }

  ssize_t bytes_read = read(file, get_buffer(safe_buffer), PAGE_SIZE);
  if (PAGE_SIZE != bytes_read) {
    fprintf(stderr, "failed to read a page from file.");
    return;
  }

  *error = success;
  set_buffer_length(safe_buffer, PAGE_SIZE);
}

void write_page_to_file(int file, SafeBuffer *safe_buffer, uint64_t page_id,
                        enum FileErrorStatus *error) {
  assert_page_size(page_id);

  *error = failure;

  uint64_t offset_from_start = (page_id * PAGE_SIZE);
  off_t lseek_offset = lseek(file, offset_from_start, SEEK_SET);
  if (-1 == lseek_offset) {
    fprintf(stderr, "lseek failed while writing.");
    return;
  }

  size_t bytes_written =
      write(file, get_buffer(safe_buffer), get_buffer_length(safe_buffer));
  if (bytes_written != get_buffer_length(safe_buffer)) {
    fprintf(stderr, "failed to write to file.");
  }
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
