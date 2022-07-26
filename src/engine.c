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

typedef bool (*DatabasePredicate)(const DataPage *data_page, uint64_t index,
                                  const void *inner_arguments,
                                  enum FileErrorStatus *error);
typedef struct {
  DatabasePredicate predicate;
  void *inner_arguments;
} DatabasePredicateClosure;

typedef struct {
  uint32_t record_length;
} SpaceEnough;

typedef struct {
  const char *key;
} KeyMatch;

static uint64_t hash(const char *key, size_t no_data_pages);

static bool find_element(int fd, DatabasePredicateClosure *closure,
                         uint64_t no_pages, uint64_t from_index,
                         uint64_t *index, enum FileErrorStatus *error);

static bool is_key_match(const DataPage *data_page, uint64_t index,
                         const void *inner_arguments,
                         enum FileErrorStatus *error);

static bool is_space_enough(const DataPage *data_page, uint64_t index,
                            const void *inner_arguments,
                            enum FileErrorStatus *error);

static uint64_t no_pages(int fd, enum FileErrorStatus *error);

// API Implementation
int open_database(char *path, enum FileErrorStatus *error) {
  *error = success;
  int fd = open_database_file(path, false, error);
  if (failure == *error) {
    *error = failure;
    return -1;
  }
  return fd;
}

void create_database(char *path, uint64_t no_elements,
                     enum FileErrorStatus *error) {
  create_database_file(path, no_elements, error);
}

bool query_element(int fd, const char *key, Record *record,
                   enum FileErrorStatus *error) {
  *error = success;
  bool return_value = false;

  uint64_t number_pages = no_pages(fd, error);
  if (failure == *error) {
    goto cleanup_1;
  }

  uint64_t index = hash(key, number_pages - 1);
  KeyMatch key_match = {.key = key};
  DatabasePredicateClosure closure = {.predicate = is_key_match,
                                      .inner_arguments = &key_match};
  bool found = find_element(fd, &closure, number_pages, index, &index, error);

  if (!found) {
    goto cleanup_1;
  }

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    goto cleanup_2;
  }

  read_page_into_buffer(fd, index, safe_buffer, error);
  if (failure == *error) {
    goto cleanup_3;
  }

  DataPage data_page = create_data_page(safe_buffer);
  data_page_find_entry(&data_page, key, record);
  return_value = true;

cleanup_3:
  free_page_buffer(safe_buffer);
cleanup_2:
  unlock_page(fd, index, error);
cleanup_1:
  close_database_file(fd, error);
cleanup_0:
  return return_value;
}

void insert_element(int fd, const char *key, const char *value,
                    enum FileErrorStatus *error) {
  *error = success;

  uint64_t number_pages = no_pages(fd, error);
  if (failure == *error) {
    goto cleanup_1;
  }

  SafeBuffer *record_safe_buffer = allocate_record_buffer();
  if (NULL == record_safe_buffer) {
    *error = failure;
    goto cleanup_2;
  }

  Record record = record_from_data(record_safe_buffer, key, value);

  uint64_t original_index = hash(key, number_pages - 1);
  SpaceEnough space_enough = {.record_length = get_record_length(&record)};
  DatabasePredicateClosure closure = {.predicate = is_space_enough,
                                      .inner_arguments = &space_enough};
  uint64_t new_index = original_index;
  bool found = find_element(fd, &closure, number_pages, original_index,
                            &new_index, error);
  if (!found) {
    goto cleanup_2;
  }

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    goto cleanup_3;
  }

  read_page_into_buffer(fd, new_index, safe_buffer, error);
  if (failure == *error) {
    goto cleanup_4;
  }

  unlock_page(fd, new_index, error);

  delete_element(fd, key, error);
  if (failure == *error) {
    goto cleanup_4;
  }

  DataPage data_page = create_data_page(safe_buffer);
  data_page_insert_entry(&data_page, &record, original_index);
  write_page_to_file(fd, safe_buffer, new_index, false, error);

cleanup_4:
  free_page_buffer(safe_buffer);
cleanup_3:
  unlock_page(fd, new_index, error);
cleanup_2:
  free_page_buffer(record_safe_buffer);
cleanup_1:
  close_database_file(fd, error);
cleanup_0:
  return;
}

bool delete_element(int fd, const char *key, enum FileErrorStatus *error) {
  *error = success;
  bool return_value = false;

  uint64_t number_pages = no_pages(fd, error);
  if (failure == *error) {
    goto cleanup_1;
  }

  uint64_t index = hash(key, number_pages - 1);
  KeyMatch key_match = {.key = key};
  DatabasePredicateClosure closure = {.predicate = is_key_match,
                                      .inner_arguments = &key_match};
  bool found = find_element(fd, &closure, number_pages, index, &index, error);

  if (!found) {
    goto cleanup_1;
  }

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    goto cleanup_2;
  }

  read_page_into_buffer(fd, index, safe_buffer, error);
  if (failure == *error) {
    goto cleanup_3;
  }

  DataPage data_page = create_data_page(safe_buffer);
  return_value = data_page_delete_entry(&data_page, key);

  write_lock_page(fd, index, error);
  if (failure == *error) {
    goto cleanup_3;
  }
  write_page_to_file(fd, safe_buffer, index, false, error);

cleanup_3:
  free_page_buffer(safe_buffer);
cleanup_2:
  unlock_page(fd, index, error);
cleanup_1:
  close_database_file(fd, error);
cleanup_0:
  return return_value;
}

static uint64_t hash(const char *key, size_t no_data_pages) {
  size_t length = strnlen(key, 100);
  XXH64_hash_t hash = XXH64(key, length, 0);
  return (hash % no_data_pages) + 1;
}

bool is_key_match(const DataPage *data_page, uint64_t index,
                  const void *inner_arguments, enum FileErrorStatus *error) {
  *error = success;
  const KeyMatch *typed_inner_arguments = inner_arguments;

  bool is_free_page = data_page_is_free_page(data_page);
  if (is_free_page) {
    return false;
  }

  Record record;
  uint64_t original_hash = data_page_hash(data_page);
  if (original_hash == index) {
    return data_page_find_entry(data_page, typed_inner_arguments->key, &record);
  }

  return false;
}

bool is_space_enough(const DataPage *data_page, uint64_t index,
                     const void *inner_arguments, enum FileErrorStatus *error) {
  *error = success;
  const SpaceEnough *typed_inner_arguments = inner_arguments;

  size_t length = data_page_no_entries(data_page);
  if (0 == length) {
    return true;
  }

  uint64_t original_hash = data_page_hash(data_page);
  size_t free_space = data_page_free_space(data_page);
  return original_hash == index &&
         typed_inner_arguments->record_length < free_space;
}

// A read lock is kept on the found page if found
static bool find_element(int fd, DatabasePredicateClosure *closure,
                         uint64_t no_pages, uint64_t from_index,
                         uint64_t *index, enum FileErrorStatus *error) {
  *error = success;
  bool return_value = false;

  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    goto cleanup_0;
  }

  uint64_t count = 0;
  uint64_t i = from_index;

  while (count != no_pages - 1) {

    read_lock_page(fd, i, error);
    if (failure == *error) {
      goto cleanup_1;
    };

    read_page_into_buffer(fd, i, safe_buffer, error);
    if (failure == *error) {
      goto cleanup_2;
    };

    DataPage data_page = create_data_page(safe_buffer);
    bool found =
        closure->predicate(&data_page, i, closure->inner_arguments, error);
    if (failure == *error) {
      goto cleanup_2;
    }

    if (found) {
      *index = i;
      return_value = true;
      goto cleanup_1;
    }

    unlock_page(fd, i, error);
    if (failure == *error) {
      goto cleanup_1;
    };

    i = (i == no_pages - 1) ? 1 : (i + 1) % no_pages;
    ++count;
  }

cleanup_2:
  unlock_page(fd, i, error);
cleanup_1:
  free_page_buffer(safe_buffer);
cleanup_0:
  return return_value;
}

static uint64_t no_pages(int fd, enum FileErrorStatus *error) {
  SafeBuffer *safe_buffer = allocate_page_buffer();
  if (NULL == safe_buffer) {
    *error = failure;
    return 0;
  }

  read_page_into_buffer(fd, 0, safe_buffer, error);
  if (failure == *error) {
    free_page_buffer(safe_buffer);
    return 0;
  }

  HeaderPage header_page = open_header_page(safe_buffer);
  uint64_t no_pages = header_no_pages(&header_page);

  free_page_buffer(safe_buffer);
  return no_pages;
}
