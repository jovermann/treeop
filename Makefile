# Copyright (c) 2024 Johannes Overmann
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

# For GCC:
#CXXFLAGS ?= -Wall -Wextra -O3
# For clang:
WARNINGS ?= -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-padded -Wno-shorten-64-to-32 -Wno-missing-prototypes -Wno-sign-conversion -Wno-implicit-int-conversion -Wno-poison-system-directories -fcomment-block-commands=n -Wno-string-conversion -Wno-covered-switch-default -Wno-unsafe-buffer-usage -Wno-implicit-int-float-conversion -Wno-extra-semi-stmt
CXXFLAGS ?= -O3 $(WARNINGS)
#CXXFLAGS ?= -g $(WARNINGS)
CPPFLAGS ?= -pedantic
CXXSTD ?= -std=c++23 # C++23 for ranges

BUILDDIR=build
SOURCES = $(wildcard src/*.cpp)
OBJECTS = $(SOURCES:%.cpp=$(BUILDDIR)/%.o)
DEPENDS := $(SOURCES:%.cpp=$(BUILDDIR)/%.d)

TARGET = treeop
default: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $^ -o $@
	@echo "Done."

build/%.o: %.cpp build/%.d
	$(CXX) $(CXXSTD) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

build/%.d: %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXSTD) $(CPPFLAGS) -MM -MQ $@ $< -o $@

clean:
	rm -rf build $(TARGET) unit_test
	find . -name '*~' -delete

uint_test: clean
unit_test: CPPFLAGS += -D ENABLE_UNIT_TEST
unit_test: CXXFLAGS += -Wno-weak-vtables -Wno-missing-variable-declarations -Wno-exit-time-destructors -Wno-global-constructors
unit_test: $(OBJECTS)
	$(CXX) $^ -o $@
	./unit_test

test: $(TARGET)
	pytest -v

.PHONY: clean default unit_test test

ifeq ($(findstring $(MAKECMDGOALS),clean),)
-include $(DEPENDS)
endif
