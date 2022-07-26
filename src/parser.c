#include "parser.h"
#include "constants.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_COMMAND_STRING_LENGTH (10)

typedef struct command_data {
  char string[10];
  uint8_t command_len;
} CommandData;

// Order based on enum
CommandData command_data[5] = {{.string = "get", .command_len = 4},
                               {.string = "create", .command_len = 4},
                               {.string = "set", .command_len = 5},
                               {.string = "ts", .command_len = 4},
                               {.string = "del", .command_len = 4}};

static bool check_string_size(const char *string);
static bool check_strings(int command_length, char **strings);

Command parse_command(const char *command, enum FileErrorStatus *error) {
  *error = success;

  if (NULL == command) {
    *error = failure;
    fprintf(stderr, "invalid command.");
    return COMMAND_LENGTH;
  }

  for (uint32_t i = 0; i < COMMAND_LENGTH; ++i) {
    if (0 ==
        strncmp(command_data[i].string, command,
                strnlen(command_data[i].string, MAX_COMMAND_STRING_LENGTH))) {
      return (Command)i;
    }
  }

  *error = failure;
  return COMMAND_LENGTH;
}

ParsedValues parse_values(Command command, int argc, char **argv,
                          enum FileErrorStatus *error) {
  *error = success;

  ParsedValues parsed_values;
  parsed_values.command = command;
  parsed_values.path = argv[1];
  parsed_values.key = argv[3];

  if (argc != command_data[command].command_len || !check_strings(argc, argv)) {
    *error = failure;
    return parsed_values;
  }

  switch (command) {
  case COMMAND_INSERT:
    parsed_values.value = argv[4];
    break;
  case COMMAND_CREATE: {
    char *end;
    parsed_values.no_elements = strtoull(argv[4], &end, 10);
    if (parsed_values.no_elements == 0) {
      *error = failure;
      return parsed_values;
    }
    break;
  default:
    break;
  }
  }
  return parsed_values;
}

static bool check_string_size(const char *string) {
  size_t length = strnlen(string, MAX_STRING_LENGTH);
  return length > 0 && length <= MAX_STRING_LENGTH;
}

static bool check_strings(int command_length, char **strings) {
  for (uint64_t i = 2; i < command_length; ++i) {
    if (!check_string_size(strings[i])) {
      fprintf(stderr, "invalid input string length\n.");
      return false;
    }
  }
  return true;
}
