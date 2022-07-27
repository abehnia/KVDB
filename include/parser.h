#pragma once
#include "error.h"
#include <inttypes.h>

typedef enum {
  COMMAND_GET = 0,
  COMMAND_CREATE,
  COMMAND_INSERT,
  COMMAND_TIMESTAMP,
  COMMAND_DELETE,
  COMMAND_LENGTH
} Command;

typedef struct {
  const char *key;
  const char *value;
  const char *path;
  uint64_t no_elements;
  Command command;
} ParsedValues;

Command parse_command(const char *command, enum FileErrorStatus *error);

ParsedValues parse_values(Command command, int argc, char **argv,
                          enum FileErrorStatus *error);


