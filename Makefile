CXX ?= c++
PYTHON ?= python3
ARROW_DIR ?= $(shell $(PYTHON) -c 'import importlib.util as u; print(next(iter(u.find_spec("pyarrow").submodule_search_locations)))')
ARROW_LIBS = $(firstword $(wildcard $(ARROW_DIR)/libparquet.so.* $(ARROW_DIR)/libparquet.*.dylib)) $(firstword $(wildcard $(ARROW_DIR)/libarrow.so.* $(ARROW_DIR)/libarrow.*.dylib))
CXXFLAGS ?= -O3 -DNDEBUG
BIN ?= shopping-sessions

$(BIN): src/main.cpp $(wildcard src/*.hpp)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -std=c++17 -pthread -Wall -Wextra -isystem "$(ARROW_DIR)/include" src/main.cpp $(ARROW_LIBS) -Wl,-rpath,"$(ARROW_DIR)" $(LDFLAGS) -o "$@"
