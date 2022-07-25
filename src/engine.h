#pragma once
#include "file_utilities.h"
#include "record.h"

void create_database(char *path, uint64_t no_elements,
                     enum FileErrorStatus *error);
bool query_element(char *path, const char *key, Record *record, enum FileErrorStatus *error);
void insert_element(char *path, const char *key, const char *value, enum FileErrorStatus *error);
void delete_element(char *path, const char *key, enum FileErrorStatus *error);
