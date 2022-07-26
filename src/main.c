#include "engine.h"
#include "record.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
  if (argc < 4) {
    fprintf(stderr, "wrong number of arguments.\n");
    return 1;
  }
  if (0 == strncmp(argv[1], "create", 6)) {
    char *path = argv[2];
    uint64_t number_of_elements = atol(argv[3]);
    enum FileErrorStatus error = failure;
    create_database(path, number_of_elements, &error);
    if (success == error) {
      printf("successfully created database.\n");
    }
  } else if (0 == strncmp(argv[1], "insert", 6)) {
    char *path = argv[2];
    char *key = argv[3];
    char *value = argv[4];
    enum FileErrorStatus error = failure;
    insert_element(path, key, value, &error);
    if (success == error) {
      printf("successfully inserted element.\n");
    }
  } else if (0 == strncmp(argv[1], "get", 6)) {
    char *path = argv[2];
    char *key = argv[3];
    enum FileErrorStatus error = failure;
    Record record;
    query_element(path, key, &record, &error);
    if (success == error) {
      printf("value: %s\n", record_value(&record));
    }
  }
  return 0;
}
