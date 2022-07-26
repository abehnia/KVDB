#pragma once
#include "file_utilities.h"
#include "record.h"

int open_database(char *path, bool with_write_lock,
                  enum FileErrorStatus *error);
void create_database(char *path, uint64_t no_elements,
                     enum FileErrorStatus *error);
bool query_element(int fd, const char *key, Record *record,
                   enum FileErrorStatus *error);
void insert_element(int fd, const char *key, const char *value,
                    enum FileErrorStatus *error);
bool delete_element(int fd, const char *key, enum FileErrorStatus *error);
