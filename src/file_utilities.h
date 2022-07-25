#pragma once

#include "buffer_manager.h"
#include <inttypes.h>
#include <stdio.h>

#define PAGE_HEADER_SIZE_ESTIMATE (100)
#define PAGE_SIZE (4096)
#define PAGE_NO_BITS (12)
#define MAX_STRING_LENGTH (100)
#define RECORD_SIZE_ESTIMATE (MAX_STRING_LENGTH + MAX_STRING_LENGTH + 10)
#define MAX_NO_ELEMENTS ((uint64_t)1 << 55)

enum FileErrorStatus { success, failure };

int create_database_file(char *path, uint64_t no_elements);

int open_database_file(char *path, enum FileErrorStatus *error);

void read_page_into_buffer(int file, uint64_t page_id, SafeBuffer *safe_buffer,
                           enum FileErrorStatus *error);

void write_page_to_file(int file, SafeBuffer *safe_buffer, uint64_t page_id,
                        enum FileErrorStatus *error);

void read_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void write_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void unlock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void close_database_file(int fd, enum FileErrorStatus *error);
