#include "record.h"
#include "buffer_manager.h"
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <time.h>

// total_length (2 bytes) | key length (1 byte) | key (key length + 1 bytes) |
// value length (1 byte) | value (value length + 1 bytes) | timestamp first
// seconds (8 bytes) | timestamp first nanoseconds (8 bytes) | Timestamp last
// seconds (8 bytes) | Timestamp last nanoseconds (8 bytes)

#define BYTE_SIZE (8)

#define LENGTH_OFFSET (0)
#define LENGTH_SIZE (2)

#define KEY_LENGTH_SIZE (1)
#define KEY_LENGTH_OFFSET (LENGTH_OFFSET + LENGTH_SIZE)
#define KEY_OFFSET (KEY_LENGTH_OFFSET + KEY_LENGTH_SIZE)

#define VALUE_LENGTH_SIZE (1)
#define STRING_TERMINATOR_SIZE (1)

#define TIMESTAMP_SECONDS_SIZE (8)
#define TIMESTAMP_NANOSECONDS_SIZE (8)

static void update_timestamp(Record *record, const Timestamp *timestamp);
static void insert_key(Record *record, const char *key);
static void clean_record(Record *record);
static void write_data_to_buffer(uint8_t *buffer, uint64_t data, uint32_t start,
                                 uint32_t length);
static Timestamp get_timestamp();
static uint32_t value_offset(const Record *record);
static uint32_t timestamp_first_offset(const Record *record);
static uint32_t timestamp_last_offset(const Record *record);
static uint64_t read_data_from_buffer(uint8_t *buffer, uint32_t start,
                                      uint32_t length);
static void update_length(Record *record, uint16_t length);

const Record record_from_buffer(SafeBuffer *safe_buffer) {
  return (const Record){.safe_buffer = safe_buffer};
}

Record record_from_data(SafeBuffer *safe_buffer, const char *key,
                        const char *value) {
  assert(get_buffer_capacity(safe_buffer) >= RECORD_SIZE_ESTIMATE);
  assert(key);
  assert(value);
  Record record = {.safe_buffer = safe_buffer};
  clean_record(&record);
  insert_key(&record, key);
  record_update_data(&record, value);
  update_length(&record, timestamp_last_offset(&record) +
                             TIMESTAMP_SECONDS_SIZE +
                             TIMESTAMP_NANOSECONDS_SIZE + 1);
  return record;
}

// Add value and timestamp to a record with key only
void record_update_data(Record *record, const char *value) {
  Timestamp timestamp = get_timestamp();
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t length = strnlen(value, MAX_STRING_LENGTH);
  uint32_t key_length = buffer[KEY_LENGTH_OFFSET];
  uint32_t value_length_offset = value_offset(record) - 1;
  uint32_t value_old_length = buffer[value_length_offset];
  buffer[value_length_offset] = length;
  memcpy(buffer + value_length_offset, value, length);
  uint32_t t_offset = timestamp_last_offset(record);
  write_data_to_buffer(buffer, (uint64_t)timestamp.seconds, t_offset,
                       sizeof(uint64_t));
  write_data_to_buffer(buffer, (uint64_t)timestamp.nanoseconds,
                       t_offset + TIMESTAMP_SECONDS_SIZE, sizeof(uint64_t));
  uint32_t record_length = get_record_length(record);
  update_length(record, record_length + length - value_old_length);
}

const char *record_key(const Record *record) {
  assert(record);
  uint8_t *buffer = get_buffer(record->safe_buffer);
  return (char *)buffer + KEY_LENGTH_OFFSET + KEY_LENGTH_SIZE;
}

const char *record_value(const Record *record) {
  assert(record);
  uint8_t *buffer = get_buffer(record->safe_buffer);
  return (char *)buffer + value_offset(record);
}

Timestamp record_first_timestamp(const Record *record) {
  assert(record);
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t offset = timestamp_first_offset(record);
  uint64_t seconds = read_data_from_buffer(buffer, offset, sizeof(uint64_t));
  uint64_t nanoseconds = read_data_from_buffer(
      buffer, offset + TIMESTAMP_SECONDS_SIZE, sizeof(uint64_t));
  return (Timestamp){seconds, nanoseconds};
}

Timestamp record_last_timestamp(const Record *record) {
  assert(record);
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t offset = timestamp_last_offset(record);
  uint64_t seconds = read_data_from_buffer(buffer, offset, sizeof(uint64_t));
  uint64_t nanoseconds = read_data_from_buffer(
      buffer, offset + TIMESTAMP_SECONDS_SIZE, sizeof(uint64_t));
  return (Timestamp){seconds, nanoseconds};
}

uint32_t get_record_length(const Record *record) {
  assert(record);
  uint8_t *buffer = get_buffer(record->safe_buffer);
  return buffer[0];
}

const uint8_t *record_get_buffer(const Record *record) {
  assert(record);
  return get_buffer(record->safe_buffer);
}

void destroy_record(Record *record) { free_record_buffer(record->safe_buffer); }

static Timestamp get_timestamp() {
  struct timespec ts;
  timespec_get(&ts, TIME_UTC);
  return (Timestamp){.seconds = ts.tv_sec, .nanoseconds = ts.tv_nsec};
}

// Add key to a new clean record (everything set to 0)
static void insert_key(Record *record, const char *key) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t length = strnlen(key, MAX_STRING_LENGTH);
  buffer[KEY_LENGTH_OFFSET] = length;
  memcpy(buffer + KEY_OFFSET, key, length);
}

static void clean_record(Record *record) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  memset(buffer, 0, get_buffer_capacity(record->safe_buffer));
}

static uint64_t read_data_from_buffer(uint8_t *buffer, uint32_t start,
                                      uint32_t length) {
  assert(start + length < RECORD_SIZE_ESTIMATE);
  uint64_t result = 0;
  for (uint32_t i = start; i < start + length; ++i) {
    uint8_t byte = buffer[i];
    result += (byte << (sizeof(uint8_t) * (i - start)));
  }
  return result;
}

static void write_data_to_buffer(uint8_t *buffer, uint64_t data, uint32_t start,
                                 uint32_t length) {
  assert(start + length < RECORD_SIZE_ESTIMATE);
  for (uint32_t i = start; i < start + length; ++i) {
    uint8_t byte = 0xFF & data;
    data = data >> sizeof(uint8_t);
    buffer[i] = data;
  }
}

static uint32_t value_offset(const Record *record) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  return KEY_LENGTH_OFFSET + KEY_LENGTH_SIZE +
         buffer[KEY_LENGTH_SIZE + KEY_LENGTH_OFFSET] + STRING_TERMINATOR_SIZE +
         1;
}

static uint32_t timestamp_first_offset(const Record *record) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t offset = value_offset(record);
  return buffer[offset - 1] + offset + STRING_TERMINATOR_SIZE;
}

static uint32_t timestamp_last_offset(const Record *record) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  uint32_t offset = timestamp_first_offset(record);
  return offset + TIMESTAMP_SECONDS_SIZE + TIMESTAMP_NANOSECONDS_SIZE;
  ;
}

static void update_length(Record *record, uint16_t length) {
  uint8_t *buffer = get_buffer(record->safe_buffer);
  write_data_to_buffer(buffer, length, LENGTH_OFFSET, LENGTH_SIZE);
}
