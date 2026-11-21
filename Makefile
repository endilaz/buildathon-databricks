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
