#include "engine.h"
#include "buffer_manager.h"
#include "data_page.h"
#include "file_utilities.h"
#include "header_page.h"
#include "record.h"
#include "xxhash.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static uint64_t hash(const char *key, size_t no_non_header_pages);

void create_database(char *path, uint64_t no_elements,
                     enum FileErrorStatus *error) {
  create_database_file(path, no_elements, error);
}

bool query_element(char *path, const char *key, Record *record,
                   enum FileErrorStatus *error) {
  *error = failure;

  int fd = open_database_file(path, error);
  if (failure == *error) {
    return false;
  }

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    return false;
  }

  read_page_into_buffer(fd, 0, safe_buffer, error);
  if (failure == *error) {
    free_page_buffer(safe_buffer);
    return false;
  }

  HeaderPage header_page = open_header_page(safe_buffer);
  uint64_t no_pages = header_no_pages(&header_page);
  uint64_t index = hash(key, no_pages - 1);
  uint64_t count = 0;

  for (uint64_t i = index; count != no_pages - 1;) {
    read_page_into_buffer(fd, i, safe_buffer, error);
    if (failure == *error) {
      free_page_buffer(safe_buffer);
      return false;
    };

    DataPage data_page = create_data_page(safe_buffer);
    bool is_free_page = data_page_is_free_page(&data_page);
    uint64_t original_hash = data_page_hash(&data_page);
    if (is_free_page) {
      *error = success;
      free_page_buffer(safe_buffer);
      return false;
    }

    if (original_hash == index) {
      bool found = data_page_find_entry(&data_page, key, record);
      if (found) {
        *error = success;
        free_page_buffer(safe_buffer);
        return true;
      }
    }

    i = (i == no_pages - 1) ? 1 : (i + 1) % no_pages;
    ++count;
  }

  free_page_buffer(safe_buffer);
  return false;
}

void insert_element(char *path, const char *key, const char *value,
                    enum FileErrorStatus *error) {
  *error = failure;
  int fd = open_database_file(path, error);
  if (failure == *error) {
    return;
  }

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    return;
  }

  SafeBuffer *record_safe_buffer = allocate_record_buffer();
  if (NULL == safe_buffer) {
    free_page_buffer(safe_buffer);
    return;
  }

  read_page_into_buffer(fd, 0, safe_buffer, error);
  if (failure == *error) {
    free_page_buffer(safe_buffer);
    free_page_buffer(record_safe_buffer);
    return;
  }

  Record record = record_from_data(record_safe_buffer, key, value);
  time_t bla = (time_t)record_first_timestamp(&record).seconds;
         record_value(&record), get_record_length(&record),
         asctime(gmtime(&bla)));

  HeaderPage header_page = open_header_page(safe_buffer);
  uint64_t no_pages = header_no_pages(&header_page);
  uint64_t index = hash(key, no_pages - 1);
  uint64_t count = 0;

  bool inserted = false;

  for (uint64_t i = index; (count != no_pages - 1) && !inserted;) {
    read_page_into_buffer(fd, i, safe_buffer, error);
    if (failure == *error) {
      free_page_buffer(safe_buffer);
      free_page_buffer(record_safe_buffer);
      return;
    };

    DataPage data_page = create_data_page(safe_buffer);
    size_t length = data_page_no_entries(&data_page);
    uint64_t original_hash = data_page_hash(&data_page);
    size_t free_space = data_page_free_space(&data_page);
    uint32_t record_length = get_record_length(&record);

    if (!inserted) {
      if (0 == length) {
        data_page_insert_entry(&data_page, &record, index);
        write_page_to_file(fd, safe_buffer, i, false, error);
        inserted = true;
      } else {
        if (original_hash == index && record_length < free_space) {
          data_page_insert_entry(&data_page, &record, index);
          write_page_to_file(fd, safe_buffer, i, false, error);
        }
        inserted = true;
      }
    }


    i = (i == no_pages - 1) ? 1 : (i + 1) % no_pages;
    ++count;
  }

  free_page_buffer(safe_buffer);
  free_page_buffer(record_safe_buffer);
  if (inserted) {
    *error = success;
  }
}

void delete_element(char *path, const char *key, enum FileErrorStatus *error) {
  int fd = open_database_file(path, error);
  if (failure == *error) {
    return;
  }
}

static uint64_t hash(const char *key, size_t no_non_header_pages) {
  size_t length = strnlen(key, 100);
  XXH64_hash_t hash = XXH64(key, length, 0);
  return (hash % no_non_header_pages) + 1;
}
