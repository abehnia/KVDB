#pragma once

#include "buffer_manager.h"
#include "constants.h"
#include <stdbool.h>

typedef struct {
  SafeBuffer *safe_buffer;
} Record;

typedef struct {
  uint64_t seconds;
  uint64_t nanoseconds;
} Timestamp;

const Record record_from_buffer(SafeBuffer *safe_buffer);
Record record_from_data(SafeBuffer *safe_buffer, const char *key,
                        const char *value);
const char *record_key(const Record *record);
const char *record_value(const Record *record);
Timestamp record_first_timestamp(const Record *record);
Timestamp record_last_timestamp(const Record *record);
void record_update_data(Record *record, const char *value,
                        const Timestamp *timestamp);
uint32_t get_record_length(const Record *record);
const uint8_t *record_get_buffer(const Record *record);
void destroy_record(Record *record);
Record record_clone(Record *record);
void format_timestamp_into_date(const Timestamp *timestamp, char *date_buffer, size_t len_date_buffer);
