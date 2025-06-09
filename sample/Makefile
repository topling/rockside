SHELL := $(shell which bash)
DEBUG_LEVEL ?= 2

$(info $$DEBUG_LEVEL is ${DEBUG_LEVEL})
# Figure out optimize level.
ifneq ($(DEBUG_LEVEL), 2)
  OPTIMIZE_LEVEL ?= -O2
endif
ifeq ($(DEBUG_LEVEL), 0)
  LIBROCKSDB := rocksdb
else
  LIBROCKSDB := rocksdb_debug
endif

LIB_MODE ?= shared
ifeq ($(LIB_MODE), static)
  BEG_WHOLE_ARCHIVE := -Wl,--whole-archive
  END_WHOLE_ARCHIVE := -Wl,--no-whole-archive
  INDIRECT_LIB_FOR_STATIC_LD := -lgomp -lcurl -laio -lrt -luring -ldl
  INDIRECT_LIB_FOR_STATIC_LD += -lbz2 -llz4 -lz -lsnappy
else ifneq ($(LIB_MODE), shared)
  $(error LIB_MODE must be static or shared)
endif
LIB_STD_CXX_FS := -lstdc++fs # old g++(8.x,9.x) needs this

# topling-core is topling private
ifneq (,$(wildcard ../../topling-core))
  TOPLING_CORE_DIR := ../../topling-core
else
  # topling-zip is topling public
  ifeq (,$(wildcard ../../topling-zip))
    $(error ../../topling-zip is not present)
  endif
  TOPLING_CORE_DIR := ../../topling-zip
endif
COMPILER := $(shell set -e; tmpfile=`mktemp -u compiler-XXXXXX`; \
                    ${CXX} ${TOPLING_CORE_DIR}/tools/configure/compiler.cpp -o $${tmpfile}.exe; \
                    ./$${tmpfile}.exe && rm -f $${tmpfile}*)
UNAME_MachineSystem := $(shell uname -m -s | sed 's:[ /]:-:g')
WITH_BMI2 ?= $(shell bash ${TOPLING_CORE_DIR}/cpu_has_bmi2.sh)
BUILD_NAME := ${UNAME_MachineSystem}-${COMPILER}-bmi2-${WITH_BMI2}
BUILD_ROOT := build/${BUILD_NAME}
ifeq (${DEBUG_LEVEL}, 0)
  BUILD_TYPE_SIG := r
  OBJ_DIR := ${BUILD_ROOT}/rls
endif
ifeq (${DEBUG_LEVEL}, 1)
  BUILD_TYPE_SIG := a
  OBJ_DIR := ${BUILD_ROOT}/afr
endif
ifeq (${DEBUG_LEVEL}, 2)
  BUILD_TYPE_SIG := d
  OBJ_DIR := ${BUILD_ROOT}/dbg
endif

TOPLINGDB_HOME := ../../..
ROCKSIDE_HOME := ..

INCS := -I${TOPLINGDB_HOME}/include -I${TOPLINGDB_HOME} \
        -I${ROCKSIDE_HOME}/src \
        -I${TOPLING_CORE_DIR}/src \
        -I${TOPLING_CORE_DIR}/boost-include

CXXFLAGS += -std=gnu++17
CXXFLAGS += ${INCS}
LDFLAGS += -ljemalloc
LDFLAGS += -L${TOPLINGDB_HOME} \
      ${BEG_WHOLE_ARCHIVE} \
           -l${LIBROCKSDB} \
           -L${TOPLING_CORE_DIR}/${BUILD_ROOT}/lib_${LIB_MODE} \
           -lterark-{zbs,fsa,core}-${COMPILER}-${BUILD_TYPE_SIG} \
      ${END_WHOLE_ARCHIVE} \
           ${LIB_STD_CXX_FS} \
           ${INDIRECT_LIB_FOR_STATIC_LD} \
           -lpthread


BINARY_EXE := $(patsubst %.cc, $(OBJ_DIR)/%, $(wildcard *.cc))
DEPFILES_CC := $(patsubst %.cc, $(OBJ_DIR)/%.cc.d, $(wildcard *.cc))

all: ${BINARY_EXE}

# link
%: %.o
	$(CXX) $< -o $@ $(LDFLAGS)

$(OBJ_DIR)/%.o: %.cc
	@mkdir -p $(dir $@)
	$(CXX) -c -g3 $(CXXFLAGS) -o $@ $<

$(OBJ_DIR)/%.cc.d: %.cc
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -MM -MT'$@' -MT'$(<:.cc=.o)' "$<" -o '$@'

-include ${DEPFILES_CC}
