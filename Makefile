CXX ?= c++
CXXFLAGS ?= -O3 -DNDEBUG
ARCHFLAGS ?= -march=native
BIN ?= shopping-sessions

$(BIN): src/main.cpp $(wildcard src/*.hpp)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(ARCHFLAGS) -std=c++20 -pthread -Wall -Wextra src/main.cpp $(LDFLAGS) -o "$@"

clean:
	rm -f "$(BIN)"
.PHONY: clean

test: tests/parse_test.cpp $(wildcard src/*.hpp)
	$(CXX) -O2 $(ARCHFLAGS) -std=c++20 -Wall -Wextra tests/parse_test.cpp -o tests/parse_test && ./tests/parse_test
.PHONY: test

# Undefined-behaviour build: catches misaligned references (x86 faults on them;
# emulators and arm64 do not) and other UB. Used by scripts/check.sh.
ubsan: src/main.cpp $(wildcard src/*.hpp)
	$(CXX) -O1 -g -std=c++20 -pthread -fsanitize=alignment,undefined -fno-sanitize=vptr -fno-sanitize-recover=all src/main.cpp -o shopping-sessions-ubsan
.PHONY: ubsan
