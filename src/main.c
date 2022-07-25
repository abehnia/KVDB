#include "engine.h"
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
  }
  return 0;
}
