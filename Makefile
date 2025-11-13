# DHT Crawler Makefile

CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c99 -D_POSIX_C_SOURCE=200809L
INCLUDES = -Iinclude -Ilib/wbpxre-dht -Ilib/bencode-c -Ilib/cJSON -Ilib/civetweb/include -Ilib/libbloom -Ilib/uthash/src
LDFLAGS = -luv -lsqlite3 -lpthread -lssl -lcrypto -ldl -lm -lurcu lib/libbloom/build/libbloom.a

# Directories
SRC_DIR = src
INC_DIR = include
LIB_DIR = lib
BUILD_DIR = build
DATA_DIR = data

# Source files
SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

# Library sources
LIB_OBJS = $(BUILD_DIR)/wbpxre_dht.o \
           $(BUILD_DIR)/wbpxre_routing.o \
           $(BUILD_DIR)/wbpxre_protocol.o \
           $(BUILD_DIR)/wbpxre_worker.o \
           $(BUILD_DIR)/bencode.o \
           $(BUILD_DIR)/cJSON.o \
           $(BUILD_DIR)/civetweb.o

# Target executable
TARGET = dht_crawler

.PHONY: all clean dirs install-deps libbloom

all: dirs libbloom $(TARGET)

dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(DATA_DIR)

$(TARGET): $(OBJS) $(LIB_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/wbpxre_dht.o: $(LIB_DIR)/wbpxre-dht/wbpxre_dht.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/wbpxre_routing.o: $(LIB_DIR)/wbpxre-dht/wbpxre_routing.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/wbpxre_protocol.o: $(LIB_DIR)/wbpxre-dht/wbpxre_protocol.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/wbpxre_worker.o: $(LIB_DIR)/wbpxre-dht/wbpxre_worker.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/bencode.o: $(LIB_DIR)/bencode-c/bencode.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/cJSON.o: $(LIB_DIR)/cJSON/cJSON.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/civetweb.o: $(LIB_DIR)/civetweb/src/civetweb.c
	$(CC) $(CFLAGS) $(INCLUDES) -DNO_SSL -c $< -o $@

libbloom:
	@echo "Building libbloom..."
	@cd $(LIB_DIR)/libbloom && $(MAKE)

clean:
	rm -rf $(BUILD_DIR)
	rm -f $(TARGET)
	@cd $(LIB_DIR)/libbloom && $(MAKE) clean

install-deps:
	@echo "Installing dependencies..."
	@echo "Please ensure the following packages are installed:"
	@echo "  - libuv-dev"
	@echo "  - libsqlite3-dev"
	@echo "  - libssl-dev"
	@echo "  - build-essential"
	@echo "  - liburcu (userspace-rcu on Arch, liburcu-dev on Debian/Ubuntu)"

# Development helpers
run: $(TARGET)
	./$(TARGET)

debug: CFLAGS += -g -DDEBUG
debug: clean all

asan: CFLAGS += -g -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer
asan: LDFLAGS += -fsanitize=address -fsanitize=undefined
asan: clean all

valgrind: debug
	valgrind --leak-check=full --show-leak-kinds=all ./$(TARGET)

.PHONY: run debug asan valgrind
