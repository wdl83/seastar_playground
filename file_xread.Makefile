include Makefile.defs
include Makefile.seastar

OBJ_DIR = obj_$(BUILD)
TARGET = file_xread

CXXSRCS += file_xread.cpp

include Makefile.rules

clean:
	rm $(OBJ_DIR) -rf
