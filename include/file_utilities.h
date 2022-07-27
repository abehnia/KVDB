#pragma once

#include "buffer_manager.h"
#include "constants.h"
#include "error.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>

int create_database_file(char *path, uint64_t no_elements,
                         enum FileErrorStatus *error);

int open_database_file(char *path, bool with_write_lock,
                       enum FileErrorStatus *error);

void read_page_into_buffer(int file, uint64_t page_id, SafeBuffer *safe_buffer,
                           enum FileErrorStatus *error);

void write_page_to_file(int file, SafeBuffer *safe_buffer, uint64_t page_id,
                        bool append_only, enum FileErrorStatus *error);

void read_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void write_lock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void unlock_page(int file, uint64_t page_id, enum FileErrorStatus *error);

void close_database_file(int fd, enum FileErrorStatus *error);

void locked_read_page_into_buffer(int file, uint64_t page_id,
                                  SafeBuffer *safe_buffer,
                                  enum FileErrorStatus *error);

void locked_write_page_to_file(int file, SafeBuffer *safe_buffer,
                               uint64_t page_id, bool append_only,
                               enum FileErrorStatus *error);
