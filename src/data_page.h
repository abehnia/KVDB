#pragma once

#include "buffer_manager.h"
#include "constants.h"
#include "record.h"
#include <stdbool.h>

typedef struct {
  SafeBuffer *safe_buffer;
} DataPage;

DataPage create_data_page(SafeBuffer *safe_buffer);
size_t data_page_free_space(const DataPage *data_page);
size_t data_page_no_entries(const DataPage *data_page);
bool data_page_is_free_page(const DataPage *data_page);
bool data_page_find_entry(const DataPage *data_page, const char *key,
                          Record *record);
bool data_page_delete_entry(DataPage *data_page, const char *key,
                            Record *record);
bool data_page_insert_entry(DataPage *data_page, const Record *record,
                            uint64_t hash);
uint64_t data_page_hash(const DataPage *data_page);
DataPage data_page_from_data(SafeBuffer *safe_buffer, uint64_t page_id);
const uint8_t *data_page_buffer(DataPage *data_page);
void destroy_data_page(DataPage *data_page);
