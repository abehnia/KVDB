#include "data_page.h"
#include "buffer_manager.h"
#include "record.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

// hash (8 bytes) | free_page (1 byte) | no_entries (2 bytes) | free space (2
// bytes) | data (4083 bytes)

#define BYTE_SIZE (8)

#define HASH_SIZE (8)
#define HASH_OFFSET (0)

#define FREE_PAGE_SIZE (1)
#define FREE_PAGE_OFFSET (HASH_OFFSET + HASH_SIZE)

#define NO_ENTRIES_SIZE (2)
#define NO_ENTRIES_OFFSET (FREE_PAGE_SIZE + FREE_PAGE_OFFSET)

#define FREE_SPACE_SIZE (2)
#define FREE_SPACE_OFFSET (NO_ENTRIES_OFFSET + NO_ENTRIES_SIZE)

#define DATA_OFFSET (FREE_SPACE_OFFSET + FREE_SPACE_SIZE)

static uint64_t transform_bytes_to_data(const uint8_t *buffer, uint32_t offset,
                                        uint32_t size);
static void assert_data_page(const DataPage *data_page);
static uint32_t free_spot(const DataPage *data_page);
static void update_no_entries(DataPage *data_page, size_t no_entries);
static void update_is_free_page(DataPage *data_page, bool is_free);
static void update_free_space(DataPage *data_page, size_t free_space);
static void update_hash(DataPage *data_page, uint64_t hash);

DataPage data_page_from_data(SafeBuffer *safe_buffer, uint64_t page_id) {
  assert(safe_buffer);
  safe_buffer->length = DATA_PAGE_SIZE;
  uint8_t *buffer = get_buffer(safe_buffer);
  memset(buffer, 0, get_buffer_capacity(safe_buffer));
  DataPage data_page = {.safe_buffer = safe_buffer};
  update_is_free_page(&data_page, true);
  update_free_space(&data_page, DATA_PAGE_SIZE - DATA_OFFSET);
  update_hash(&data_page, page_id);
  return data_page;
}

DataPage create_data_page(SafeBuffer *safe_buffer) {
  assert(get_buffer_capacity(safe_buffer) >= DATA_PAGE_SIZE);
  return (DataPage){.safe_buffer = safe_buffer};
}

size_t data_page_free_space(const DataPage *data_page) {
  assert_data_page(data_page);
  const uint8_t *buffer = get_buffer(data_page->safe_buffer);
  return (size_t)transform_bytes_to_data(buffer, FREE_SPACE_OFFSET,
                                         FREE_SPACE_SIZE);
}

size_t data_page_no_entries(const DataPage *data_page) {
  assert_data_page(data_page);
  const uint8_t *buffer = get_buffer(data_page->safe_buffer);
  return (size_t)transform_bytes_to_data(buffer, NO_ENTRIES_OFFSET,
                                         NO_ENTRIES_SIZE);
}

bool data_page_is_free_page(const DataPage *data_page) {
  assert_data_page(data_page);
  const uint8_t *buffer = get_buffer(data_page->safe_buffer);
  return (bool)transform_bytes_to_data(buffer, FREE_PAGE_OFFSET,
                                       FREE_PAGE_SIZE);
}

uint64_t data_page_hash(const DataPage *data_page) {
  assert_data_page(data_page);
  const uint8_t *buffer = get_buffer(data_page->safe_buffer);
  return (uint64_t)transform_bytes_to_data(buffer, HASH_OFFSET, HASH_SIZE);
}

static bool find_entry(const DataPage *data_page, const char *key,
                       Record *record, uint32_t *index) {
  uint32_t data_offset = DATA_OFFSET;
  uint8_t *buffer = get_buffer(data_page->safe_buffer);

  uint32_t i = data_offset;
  while (data_offset < DATA_PAGE_SIZE && buffer[data_offset] != 0) {
    SafeBuffer safe_buffer = (SafeBuffer){.buffer = buffer + data_offset,
                                          .length = buffer[data_offset],
                                          .capacity = buffer[data_offset]};
    Record local_record = record_from_buffer(&safe_buffer);
    const char *local_key = record_key(&local_record);
    int cmp = strncmp(local_key, key, MAX_STRING_LENGTH);
    if (0 == cmp) {
      *record = local_record;
      *index = i;
      return true;
    }
    i += buffer[data_offset];
  }

  return false;
}

bool data_page_find_entry(const DataPage *data_page, const char *key,
                          Record *record) {
  assert_data_page(data_page);
  assert(key);
  uint32_t _index;

  return find_entry(data_page, key, record, &_index);
}

bool data_page_delete_entry(DataPage *data_page, const char *key) {
  assert_data_page(data_page);
  assert(key);

  Record _record;
  uint32_t index;
  bool found_entry = find_entry(data_page, key, &_record, &index);
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  if (found_entry) {
    uint8_t *entry_start = buffer + index;
    uint8_t *next_entry = entry_start + buffer[index];
    uint32_t copy_length = buffer[index];
    memmove(entry_start, next_entry, copy_length);
    size_t no_entries = data_page_no_entries(data_page);
    update_no_entries(data_page, no_entries - 1);
    size_t free_space = data_page_free_space(data_page);
    update_free_space(data_page, free_space + copy_length);
    uint32_t first_free_spot = free_spot(data_page);
    memset(buffer + first_free_spot, 0, copy_length);
    return true;
  }
  return false;
}

bool data_page_insert_entry(DataPage *data_page, const Record *record,
                            uint64_t hash) {
  assert_data_page(data_page);
  assert(record);

  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  size_t free_space = data_page_free_space(data_page);
  uint32_t record_length = get_record_length(record);
  if (record_length > free_space) {
    return false;
  }
  uint32_t first_free_spot = free_spot(data_page);
  memcpy(buffer + first_free_spot, record_get_buffer(record), record_length);
  size_t no_entries = data_page_no_entries(data_page);
  update_no_entries(data_page, no_entries + 1);
  if (1 == no_entries) {
    update_hash(data_page, hash);
  }
  update_is_free_page(data_page, false);
  update_free_space(data_page, free_space - record_length);
  return true;
}

void destroy_data_page(DataPage *data_page) {
  assert_data_page(data_page);
  free_page_buffer(data_page->safe_buffer);
}

const uint8_t *data_page_buffer(DataPage *data_page) {
  assert_data_page(data_page);
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  return buffer;
}

static void assert_data_page(const DataPage *data_page) {
  assert(data_page);
  assert(data_page->safe_buffer);
}

static uint64_t transform_bytes_to_data(const uint8_t *buffer, uint32_t offset,
                                        uint32_t size) {
  uint64_t return_value = 0;
  for (uint64_t i = offset; i < (offset + size); ++i) {
    return_value += ((uint64_t)buffer[i] << (BYTE_SIZE * (i - offset)));
  }
  return return_value;
}

static void write_data_to_buffer(uint8_t *buffer, uint32_t offset,
                                 uint32_t size, uint64_t data) {
  for (uint64_t i = offset; i < (offset + size); ++i) {
    buffer[i] = data & 0xFF;
    data = data >> BYTE_SIZE;
  }
}

static uint32_t free_spot(const DataPage *data_page) {
  size_t free_space = data_page_free_space(data_page);
  assert(free_space > 0);
  return (DATA_PAGE_SIZE - free_space);
}

static void update_no_entries(DataPage *data_page, size_t no_entries) {
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  write_data_to_buffer(buffer, NO_ENTRIES_OFFSET, NO_ENTRIES_SIZE, no_entries);
}

static void update_is_free_page(DataPage *data_page, bool is_free) {
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  write_data_to_buffer(buffer, FREE_PAGE_OFFSET, FREE_PAGE_SIZE, is_free);
}

static void update_free_space(DataPage *data_page, size_t free_space) {
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  write_data_to_buffer(buffer, FREE_SPACE_OFFSET, FREE_SPACE_SIZE, free_space);
}

static void update_hash(DataPage *data_page, uint64_t hash) {
  uint8_t *buffer = get_buffer(data_page->safe_buffer);
  write_data_to_buffer(buffer, HASH_OFFSET, HASH_SIZE, hash);
}
