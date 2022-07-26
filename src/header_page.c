#include "header_page.h"
#include "buffer_manager.h"
#include "buffer_utilities.h"
#include <assert.h>
#include <stdint.h>
#include <string.h>

#define BYTE_SIZE (8)

#define PAGE_ID_SIZE (8)
#define PAGE_ID_OFFSET (0)

#define VERSION_SIZE (8)
#define VERSION_OFFSET (PAGE_ID_OFFSET + PAGE_ID_SIZE)

#define NO_PAGES_SIZE (8)
#define NO_PAGES_OFFSET (VERSION_OFFSET + VERSION_SIZE)

// page id (8 bytes) | database_version (8 bytes) | no_pages(8 bytes) | reserved
// (4072 bytes)

static void update_page_id(HeaderPage *header_page, size_t free_space);
static void update_version(HeaderPage *header_page, uint64_t version);
static void update_no_pages(HeaderPage *header_page, uint64_t no_pages);
static void assert_header_page(const HeaderPage *header_page);

HeaderPage create_header_page(SafeBuffer *safe_buffer, uint64_t no_pages) {
  assert(safe_buffer);
  uint8_t *buffer = get_buffer(safe_buffer);
  memset(buffer, 0, get_buffer_capacity(safe_buffer));
  safe_buffer->length = HEADER_PAGE_SIZE;
  HeaderPage header_page = {.safe_buffer = safe_buffer};
  update_page_id(&header_page, 0);
  update_version(&header_page, DATABASE_VERSION);
  update_no_pages(&header_page, no_pages);
  return header_page;
}

HeaderPage open_header_page(SafeBuffer *safe_buffer) {
  assert(get_buffer_capacity(safe_buffer) >= HEADER_PAGE_SIZE);
  return (HeaderPage){.safe_buffer = safe_buffer};
}

uint64_t header_no_pages(const HeaderPage *header_page) {
  assert_header_page(header_page);
  const uint8_t *buffer = get_buffer(header_page->safe_buffer);
  return (uint64_t)read_data_from_buffer(buffer, NO_PAGES_OFFSET,
                                           NO_PAGES_SIZE);
}

uint64_t header_page_id(const HeaderPage *header_page) {
  assert_header_page(header_page);
  const uint8_t *buffer = get_buffer(header_page->safe_buffer);
  return (uint64_t)read_data_from_buffer(buffer, PAGE_ID_OFFSET,
                                           PAGE_ID_SIZE);
}

uint64_t header_version(const HeaderPage *header_page) {
  assert_header_page(header_page);
  const uint8_t *buffer = get_buffer(header_page->safe_buffer);
  return (uint64_t)read_data_from_buffer(buffer, VERSION_OFFSET,
                                           VERSION_SIZE);
}

const uint8_t *header_page_buffer(HeaderPage *header_page) {
  assert_header_page(header_page);
  uint8_t *buffer = get_buffer(header_page->safe_buffer);
  return buffer;
}

void destroy_header_page(HeaderPage *header_page) {
  assert_header_page(header_page);
  free_page_buffer(header_page->safe_buffer);
}

static void update_page_id(HeaderPage *header_page, uint64_t page_id) {
  uint8_t *buffer = get_buffer(header_page->safe_buffer);
  write_data_to_buffer(buffer, PAGE_ID_OFFSET, PAGE_ID_SIZE, page_id);
}

static void update_version(HeaderPage *header_page, uint64_t version) {
  uint8_t *buffer = get_buffer(header_page->safe_buffer);
  write_data_to_buffer(buffer, VERSION_OFFSET, VERSION_SIZE, version);
}

static void update_no_pages(HeaderPage *header_page, uint64_t no_pages) {
  uint8_t *buffer = get_buffer(header_page->safe_buffer);
  write_data_to_buffer(buffer, NO_PAGES_OFFSET, NO_PAGES_SIZE, no_pages);
}

static void assert_header_page(const HeaderPage *header_page) {
  assert(header_page);
  assert(header_page->safe_buffer);
}
