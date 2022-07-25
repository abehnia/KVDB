#pragma once

#include "buffer_manager.h"
#include <stdbool.h>

#define HEADER_PAGE_SIZE (4096)
#define DATABASE_VERSION (3834052067ULL)

typedef struct {
  SafeBuffer *safe_buffer;
} HeaderPage;

HeaderPage create_header_page(SafeBuffer *safe_buffer, uint64_t no_pages);
HeaderPage open_header_page(SafeBuffer *safe_buffer);
uint64_t header_page_id(const HeaderPage *header_page);
uint64_t header_no_pages(const HeaderPage *header_page);
uint64_t header_version(const HeaderPage *header_page);
const uint8_t *header_page_buffer(HeaderPage *header_page);
void destroy_header_page(HeaderPage *header_page);
