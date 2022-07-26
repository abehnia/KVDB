#include "engine.h"
#include "record.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static uint64_t get_most_significant(uint64_t nanoseconds) {
  while (nanoseconds >= 1000) {
    nanoseconds /= 10;
  }
  return nanoseconds;
}

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
    int fd = open_database(path, &error);
    if (failure == error) {
      exit(1);
    }
    insert_element(fd, key, value, &error);
    if (success == error) {
      printf("successfully inserted element.\n");
    }
  } else if (0 == strncmp(argv[1], "get", 6)) {
    char *path = argv[2];
    char *key = argv[3];
    enum FileErrorStatus error = failure;
    Record record;
    int fd = open_database(path, &error);
    if (failure == error) {
      exit(1);
    }
    bool found = query_element(fd, key, &record, &error);
    if (success == error) {
      if (found) {
        printf("value: %s\n", record_value(&record));
      } else {
        printf("key not found.\n");
      }
    }
  } else if (0 == strncmp(argv[1], "timestamp", 9)) {
    char *path = argv[2];
    char *key = argv[3];
    enum FileErrorStatus error = failure;
    Record record;
    int fd = open_database(path, &error);
    if (failure == error) {
      exit(1);
    }
    bool found = query_element(fd, key, &record, &error);
    if (success == error) {
      if (found) {
        Timestamp first = record_first_timestamp(&record);
        Timestamp last = record_last_timestamp(&record);
        char first_buff[100];
        char second_buff[100];
        strftime(first_buff, sizeof(first_buff), "%F %T",
                 gmtime((time_t *)&first.seconds));
        strftime(second_buff, sizeof(second_buff), "%F %T",
                 gmtime((time_t *)&last.seconds));
        printf("first_timestamp: %s.%03ld last_timestamp: %s.%03ld\n",
               first_buff, get_most_significant(first.nanoseconds), second_buff,
               get_most_significant(last.nanoseconds));
      } else {
        printf("key not found.\n");
      }
    }
  }
  return 0;
}
