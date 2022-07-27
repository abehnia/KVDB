#pragma once

#include "inttypes.h"

uint64_t read_data_from_buffer(const uint8_t *buffer, uint32_t offset,
                                uint32_t size);
void write_data_to_buffer(uint8_t *buffer, uint32_t offset, uint32_t size,
                          uint64_t data);
