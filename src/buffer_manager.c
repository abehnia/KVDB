#include "buffer_manager.h"
#include "file_utilities.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>

#define POOL_SIZE 2

typedef struct safe_buffer {
  size_t length;
  size_t capacity;
  uint8_t *buffer;
  uint8_t index;
} SafeBuffer;

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

static PagePoolEntry page_pool[POOL_SIZE] = {0};
static RecordPoolEntry record_pool[POOL_SIZE] = {0};

void set_buffer_length(SafeBuffer *safe_buffer, size_t length) {
  assert(safe_buffer);
  assert(length < safe_buffer->capacity);
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
      safe_buffer->index = i;
      safe_buffer->capacity = PAGE_SIZE;
      safe_buffer->buffer = pool_entry->buffer;
      pool_entry->allocated = true;
      return safe_buffer;
    }
  }
  fprintf(stderr, "cannot allocate page inside buffer pool");
  return NULL;
}

void free_page_buffer(SafeBuffer *buffer) {
  assert(buffer != NULL);
  assert(buffer->index < POOL_SIZE);
  assert(true == page_pool[buffer->index].allocated);
  PagePoolEntry *pool_entry = page_pool + buffer->index;
  pool_entry->allocated = false;
}

SafeBuffer *allocate_record_buffer(void) {
  for (uint32_t i = 0; i < POOL_SIZE; ++i) {
    RecordPoolEntry *pool_entry = record_pool + i;
    if (false == record_pool->allocated) {
      SafeBuffer *safe_buffer = &record_pool->safe_buffer;
      safe_buffer->index = i;
      safe_buffer->capacity = RECORD_SIZE_ESTIMATE;
      safe_buffer->buffer = pool_entry->buffer;
      pool_entry->allocated = true;
      return safe_buffer;
    }
  }
  fprintf(stderr, "cannot allocate record inside buffer pool");
  return NULL;
}

void free_record_buffer(SafeBuffer *buffer) {
  assert(buffer != NULL);
  assert(buffer->index < POOL_SIZE);
  assert(true == record_pool[buffer->index].allocated);
  RecordPoolEntry *pool_entry = record_pool + buffer->index;
  pool_entry->allocated = false;
}
