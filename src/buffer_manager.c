#include "../include/buffer_manager.h"
#include "../include/file_utilities.h"
#include "../include/record.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>

#define POOL_SIZE 4

typedef struct page_pool_entry {
  uint8_t buffer[PAGE_SIZE];
  SafeBuffer safe_buffer;
  bool allocated;
} PagePoolEntry;

typedef struct record_pool_entry {
  uint8_t buffer[RECORD_SIZE_ESTIMATE];
  SafeBuffer safe_buffer;
  bool allocated;
} RecordPoolEntry;

static PagePoolEntry page_pool[POOL_SIZE];
static RecordPoolEntry record_pool[POOL_SIZE];

void set_buffer_length(SafeBuffer *safe_buffer, size_t length) {
  assert(safe_buffer);
  assert(length <= safe_buffer->capacity);
  safe_buffer->length = length;
}

size_t get_buffer_length(SafeBuffer *safe_buffer) {
  assert(safe_buffer);
  return safe_buffer->length;
}

size_t get_buffer_capacity(SafeBuffer *safe_buffer) {
  assert(safe_buffer);
  return safe_buffer->capacity;
}

uint8_t *get_buffer(SafeBuffer *safe_buffer) {
  assert(safe_buffer);
  return safe_buffer->buffer;
}

SafeBuffer *allocate_page_buffer(void) {
  for (uint32_t i = 0; i < POOL_SIZE; ++i) {
    PagePoolEntry *pool_entry = page_pool + i;
    if (false == pool_entry->allocated) {
      SafeBuffer *safe_buffer = &pool_entry->safe_buffer;
      safe_buffer->capacity = PAGE_SIZE;
      safe_buffer->buffer = pool_entry->buffer;
      pool_entry->allocated = true;
      return safe_buffer;
    }
  }
  fprintf(stderr, "cannot allocate page inside buffer pool.\n");
  return NULL;
}

void free_page_buffer(SafeBuffer *buffer) {
  assert(buffer != NULL);
  for (uint32_t i = 0; i < POOL_SIZE; ++i) {
    PagePoolEntry *pool_entry = page_pool + i;
    if (&pool_entry->safe_buffer == buffer) {
      assert(pool_entry->allocated);
      pool_entry->allocated = false;
    }
  }
}

SafeBuffer *allocate_record_buffer(void) {
  for (uint32_t i = 0; i < POOL_SIZE; ++i) {
    RecordPoolEntry *pool_entry = record_pool + i;
    if (false == pool_entry->allocated) {
      SafeBuffer *safe_buffer = &pool_entry->safe_buffer;
      safe_buffer->capacity = RECORD_SIZE_ESTIMATE;
      safe_buffer->buffer = pool_entry->buffer;
      pool_entry->allocated = true;
      return safe_buffer;
    }
  }
  fprintf(stderr, "cannot allocate record inside buffer pool.\n");
  return NULL;
}

void free_record_buffer(SafeBuffer *buffer) {
  assert(buffer != NULL);
  for (uint32_t i = 0; i < POOL_SIZE; ++i) {
    RecordPoolEntry *record_entry = record_pool + i;
    if (&record_entry->safe_buffer == buffer) {
      assert(record_entry->allocated);
      record_entry->allocated = false;
    }
  }
}
