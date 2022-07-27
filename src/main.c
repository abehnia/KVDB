#include "engine.h"
#include "file_utilities.h"
#include "parser.h"
#include "record.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int main(int argc, char **argv) {

  if (argc < 4) {
    fprintf(stderr, "wrong number of arguments.\n");
    return 1;
  }

  enum FileErrorStatus error;
  Command command = parse_command(argv[1], &error);
  ParsedValues parsed_values = parse_values(command, argc, argv, &error);
  if (failure == error) {
    fprintf(stderr, "invalid input.\n");
    return 1;
  }

  if (COMMAND_CREATE == command) {
    create_database((char *)parsed_values.path, parsed_values.no_elements,
                    &error);
    if (success == error) {
      printf("successfully created database.\n");
    } else {
      printf("error in create database.\n");
    }
  }

  if (COMMAND_GET == command) {
    int fd = open_database((char *)parsed_values.path, false, &error);
    if (failure == error) {
      return 1;
    }

    Record record;
    bool found = query_element(fd, parsed_values.key, &record, &error);

    if (success == error) {
      if (found) {
        printf("value: %s\n", record_value(&record));
      } else {
        printf("cannot find element.\n");
      }
    } else {
      printf("error in find element.\n");
    }
    close_database_file(fd, &error);
  }

  if (COMMAND_INSERT == command) {
    int fd = open_database((char *)parsed_values.path, true, &error);
    if (failure == error) {
      return 1;
    }
    insert_element(fd, parsed_values.key, parsed_values.value, &error);
    if (success == error) {
      printf("successfully inserted element.\n");
    } else {
      printf("error in insert element.\n");
    }
    close_database_file(fd, &error);
  }

  if (COMMAND_DELETE == command) {
    int fd = open_database((char *)parsed_values.path, true, &error);
    if (failure == error) {
      return 1;
    }

    Record record;
    bool found = delete_element(fd, parsed_values.key, &record, &error);
    if (success == error) {
      if (found) {
        printf("successfully deleted element.\n");
      } else {
        printf("cannot find element,\n");
      }
    } else {
      printf("error in delete element.\n");
    }
    close_database_file(fd, &error);
  }

  if (COMMAND_TIMESTAMP == command) {
    int fd = open_database((char *)parsed_values.path, false, &error);
    if (failure == error) {
      return 1;
    }

    Record record;
    bool found = query_element(fd, parsed_values.key, &record, &error);

    if (success == error) {
      if (found) {
        Timestamp first = record_first_timestamp(&record);
        Timestamp last = record_last_timestamp(&record);
        char first_buffer[100];
        char second_buffer[100];
        format_timestamp_into_date(&first, first_buffer, sizeof(first_buffer));
        format_timestamp_into_date(&last, second_buffer, sizeof(first_buffer));
        printf("first ts: %s, last ts: %s\n", first_buffer, second_buffer);
      } else {
        printf("cannot find element.\n");
      }
    } else {
      printf("error in find element.\n");
    }
    close_database_file(fd, &error);
  }

  return 0;
}
