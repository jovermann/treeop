# Copyright (c) 2024 Johannes Overmann
#
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)

WARNING_FLAGS ?= -Weverything -Wno-c++98-compat -Wno-c++98-compat-pedantic -Wno-padded -Wno-shorten-64-to-32 -Wno-missing-prototypes -Wno-sign-conversion -Wno-implicit-int-conversion -Wno-poison-system-directories -fcomment-block-commands=n -Wno-string-conversion -Wno-covered-switch-default -Wno-unsafe-buffer-usage -Wno-implicit-int-float-conversion -Wno-extra-semi-stmt
CPPFLAGS ?= -pedantic
CXXSTD ?= -std=c++23 # C++23 for ranges
BUILD ?= release
CXXFLAGS_COMMON ?= -Wall
CXXFLAGS_DEBUG ?= -O0 -g
CXXFLAGS_RELEASE ?= -O3 -DNDEBUG
PYTEST ?= pytest-3

ifeq ($(BUILD),debug)
CXXFLAGS ?= $(CXXFLAGS_COMMON) $(CXXFLAGS_DEBUG)
else ifeq ($(BUILD),release)
CXXFLAGS ?= $(CXXFLAGS_COMMON) $(CXXFLAGS_RELEASE)
else
$(error Unknown BUILD='$(BUILD)', expected debug or release)
endif

BUILDDIR=build-$(BUILD)
UNIT_TEST_BUILDDIR=build-unit-test-$(BUILD)
SOURCES = $(wildcard src/*.cpp)
OBJECTS = $(SOURCES:%.cpp=$(BUILDDIR)/%.o)
DEPENDS := $(SOURCES:%.cpp=$(BUILDDIR)/%.d)
UNIT_TEST_OBJECTS = $(SOURCES:%.cpp=$(UNIT_TEST_BUILDDIR)/%.o)
UNIT_TEST_DEPENDS := $(SOURCES:%.cpp=$(UNIT_TEST_BUILDDIR)/%.d)

TARGET = treeop
default: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $^ -o $@
	@echo "Done."

$(BUILDDIR)/%.o: %.cpp $(BUILDDIR)/%.d
	$(CXX) $(CXXSTD) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

$(BUILDDIR)/%.d: %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXSTD) $(CPPFLAGS) -MM -MQ $@ $< -o $@

clean:
	rm -rf build build-* build-unit-test $(TARGET) unit_test
	find . -name '*~' -delete

uint_test: clean
$(UNIT_TEST_BUILDDIR)/%.o: %.cpp $(UNIT_TEST_BUILDDIR)/%.d
	$(CXX) $(CXXSTD) $(CPPFLAGS) -D ENABLE_UNIT_TEST $(CXXFLAGS) -c $< -o $@

$(UNIT_TEST_BUILDDIR)/%.d: %.cpp Makefile
	@mkdir -p $(@D)
	$(CXX) $(CXXSTD) $(CPPFLAGS) -D ENABLE_UNIT_TEST -MM -MQ $@ $< -o $@

unit_test: $(UNIT_TEST_OBJECTS)
	$(CXX) $^ -o $@
	./unit_test

test: $(TARGET)
	$(PYTEST) -v

warnings:
	$(MAKE) clean
	$(MAKE) CXXFLAGS="$(CXXFLAGS_RELEASE) $(WARNING_FLAGS)" $(TARGET)

.PHONY: clean default unit_test test warnings

ifeq ($(findstring $(MAKECMDGOALS),clean),)
ifneq ($(MAKECMDGOALS),unit_test)
-include $(DEPENDS)
endif
ifneq ($(filter unit_test,$(MAKECMDGOALS)),)
-include $(UNIT_TEST_DEPENDS)
endif
endif
