# Makefile for Lightning DB FFI examples

CC = gcc
CFLAGS = -Wall -Wextra -O2 -std=c99
LDFLAGS = -L../../target/release -llightning_db_ffi

# Platform-specific settings
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    LDFLAGS += -lpthread -ldl -lm
    RPATH_FLAG = -Wl,-rpath,../../target/release
endif
ifeq ($(UNAME_S),Darwin)
    LDFLAGS += -framework Security -framework CoreFoundation
    RPATH_FLAG = -Wl,-rpath,@loader_path/../../target/release
endif

EXAMPLES = basic_usage performance_test

all: $(EXAMPLES)

# Build the Rust library first
../../target/release/liblightning_db_ffi.dylib:
	cd ../.. && cargo build --release -p lightning-db-ffi

basic_usage: basic_usage.c ../../target/release/liblightning_db_ffi.a
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(RPATH_FLAG)

performance_test: performance_test.c ../../target/release/liblightning_db_ffi.a
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(RPATH_FLAG)

run-basic: basic_usage
	rm -rf ./test_db
	./basic_usage

run-perf: performance_test
	rm -rf ./perf_test_db
	./performance_test

clean:
	rm -f $(EXAMPLES)
	rm -rf ./test_db ./perf_test_db

.PHONY: all clean run-basic run-perf