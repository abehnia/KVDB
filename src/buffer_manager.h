#pragma once
#include <inttypes.h>
#include <stdio.h>

typedef struct safe_buffer SafeBuffer;

size_t get_buffer_length(SafeBuffer *safe_buffer);
size_t get_buffer_capacity(SafeBuffer *safe_buffer);
uint8_t *get_buffer(SafeBuffer *safe_buffer);
void set_buffer_length(SafeBuffer *safe_buffer, size_t length);

SafeBuffer *allocate_page_buffer(void);
void free_page_buffer(SafeBuffer *buffer);

SafeBuffer *allocate_record_buffer(void);
void free_record_buffer(SafeBuffer *buffer);
