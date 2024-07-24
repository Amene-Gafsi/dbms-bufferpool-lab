#.SILENT:

# The thread sanitizer is disabled by default since it can cause issues when
# there are too many threads and greatly slows down your code. 
# It can however be quite useful in catching race
# conditions! If you want to enable it, switch this line with the one below.
# You may need to reduce the number of threads in some tests to not more than 100-200.
TSAN=
#TSAN=-fsanitize=thread

CFLAGSLOCAL=-g -Wall -pedantic -Iinc -fsanitize=address
CPPFLAGSLOCAL=-g -std=c++17 -Iinc -Itests -fsanitize=address
CFLAGSREMOTE=-g0 -O3 -Iinc
CPPFLAGSREMOTE=-g0 -O3 -std=c++17 -Iinc -Itests
LDFLAGS=-pthread -lgtest_main -lgtest

# Default target: build all binaries
all: clean test-ex01 test-ex02 test-ex03

# Generate corresponding binary names
BINARIES1 := $(patsubst %.cpp,%,$(wildcard tests/tests-ex01/*.cpp))
BINARIES2 := $(patsubst %.cpp,%,$(wildcard tests/tests-ex02/*.cpp))
BINARIES3 := $(patsubst %.cpp,%,$(wildcard tests/tests-ex03/*.cpp))

OBJS1 := $(patsubst %.cpp,%.o,$(wildcard tests/tests-ex01/*.cpp))
OBJS2 := $(patsubst %.cpp,%.o,$(wildcard tests/tests-ex02/*.cpp))
OBJS3 := $(patsubst %.cpp,%.o,$(wildcard tests/tests-ex03/*.cpp))
BINS1 := $(patsubst %.o,%.exe,$(wildcard tests/tests-ex01/*.o))
BINS2 := $(patsubst %.o,%.exe,$(wildcard tests/tests-ex02/*.o))
BINS3 := $(patsubst %.o,%.exe,$(wildcard tests/tests-ex03/*.o))

test-ex01: $(BINARIES1)
	@echo "Testing Exercise 1..."
	@./grading.sh ex01 5

test-ex02: $(BINARIES2)
	@echo "Testing Exercise 2..."
	@./grading.sh ex02 5

test-ex03: $(BINARIES3)
	@echo "Testing Exercise 3..."
	@./grading.sh ex03 5

build-ex01: $(BINARIES1)
build-ex02: $(BINARIES2)
build-ex03: $(BINARIES3)
build: build-ex01 build-ex02 build-ex03

tests/tests-ex01/%.o: tests/tests-ex01/%.cpp
	@g++ $(CPPFLAGSREMOTE) -c $< -o $@
tests/tests-ex02/%.o: tests/tests-ex02/%.cpp
	@g++ $(CPPFLAGSREMOTE) -c $< -o $@
tests/tests-ex03/%.o: tests/tests-ex03/%.cpp
	@g++ $(CPPFLAGSREMOTE) -c $< -o $@

prepare_obj: lib/storage_manager.o lib/buffer_manager.o lib/replacer.o tests/ $(OBJS1) $(OBJS2) $(OBJS3)

lib/%.o: lib/%.c
	@gcc $(CFLAGSREMOTE) -c $^ -o $@

tests/tests-ex01/%.exe: tests/tests-ex01/%.o
	@g++ $(CPPFLAGSREMOTE) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@
tests/tests-ex02/%.exe: tests/tests-ex02/%.o
	@g++ $(CPPFLAGSREMOTE) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@
tests/tests-ex03/%.exe: tests/tests-ex03/%.o
	@g++ $(CPPFLAGSREMOTE) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@

prepare: prepare_obj
	@rm -f lib/*.o
	@rm -f lib/*.c
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9].cpp

merge_obj: lib/storage_manager.o lib/replacer.o lib/buffer_manager.o
merge_exe: $(BINS1) $(BINS2) $(BINS3)
merge_build: merge_obj merge_exe

#####
# Pattern rule to compile binaries
$(BINARIES1): %: %.cpp lib/storage_manager.c lib/replacer.c lib/buffer_manager.c  
	@gcc $(CFLAGSLOCAL) -c lib/storage_manager.c -o lib/storage_manager.o
	@gcc $(CFLAGSLOCAL) -c lib/replacer.c -o lib/replacer.o
	@gcc $(CFLAGSLOCAL) -c lib/buffer_manager.c -o lib/buffer_manager.o
	@g++ $(CPPFLAGSLOCAL) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@

$(BINARIES2): %: %.cpp lib/storage_manager.c lib/replacer.c lib/buffer_manager.c  
	@gcc $(CFLAGSLOCAL) -c lib/storage_manager.c -o lib/storage_manager.o
	@gcc $(CFLAGSLOCAL) -c lib/replacer.c -o lib/replacer.o
	@gcc $(CFLAGSLOCAL) -c lib/buffer_manager.c -o lib/buffer_manager.o
	@g++ $(CPPFLAGSLOCAL) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@

$(BINARIES3): %: %.cpp lib/storage_manager.c lib/replacer.c lib/buffer_manager.c  
	@gcc $(CFLAGSLOCAL) -c lib/storage_manager.c -o lib/storage_manager.o
	@gcc $(CFLAGSLOCAL) -c lib/replacer.c -o lib/replacer.o
	@gcc $(CFLAGSLOCAL) -c lib/buffer_manager.c -o lib/buffer_manager.o
	@g++ $(CPPFLAGSLOCAL) $< lib/storage_manager.o lib/replacer.o lib/buffer_manager.o   $(LDFLAGS) -o $@

# Clean target: remove compiled binaries
clean:
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9]
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9][0-9]
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9].o
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9][0-9].o
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9].exe
	@rm -f tests/tests-ex0[0-9]/test-ex0[0-9]-[0-9][0-9].exe
	@rm -f tests/*.o
	@rm -f lib/*.o

bin: $(BINARIES1) $(BINARIES2) $(BINARIES3)

.PHONY: all bin clean