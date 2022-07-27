CC=gcc -O3
CCFLAGS=-Wall
LDFLAGS=
SOURCEDIR = src/
BUILDDIR = build/
SOURCES=$(wildcard $(SOURCEDIR)*.c)
OBJECTS=$(patsubst $(SOURCEDIR)%.c, $(BUILDDIR)%.o, $(SOURCES))
TARGET=kvdb

all: dir $(TARGET)

$(TARGET): $(OBJECTS) 
	$(CC) -o $@ $^ $(LDFLAGS) 

$(BUILDDIR)%.o: $(SOURCEDIR)%.c
	$(CC) $(CCFLAGS) -c $< -o $@

.PHONEY: clean dir

dir:
	mkdir -p $(BUILDDIR)

clean:
	rm -rf $(BUILDDIR) kvdb
