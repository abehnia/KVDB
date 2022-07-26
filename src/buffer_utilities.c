#include "buffer_utilities.h"
#include "constants.h"

uint64_t read_data_from_buffer(const uint8_t *buffer, uint32_t offset,
                                uint32_t size) {
  uint64_t return_value = 0;
  for (uint64_t i = offset; i < (offset + size); ++i) {
    return_value += ((uint64_t)buffer[i] << (BYTE_SIZE * (i - offset)));
  }
  return return_value;
}

void write_data_to_buffer(uint8_t *buffer, uint32_t offset, uint32_t size,
                          uint64_t data) {
  for (uint64_t i = offset; i < (offset + size); ++i) {
    buffer[i] = data & 0xFF;
    data = data >> BYTE_SIZE;
  }
}
