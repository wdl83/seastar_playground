include Makefile.defs
include Makefile.seastar

OBJ_DIR = obj_$(BUILD)
TARGET = sharded_file_xread

CXXSRCS += sharded_file_xread.cpp

include Makefile.rules

clean:
	rm $(OBJ_DIR) -rf
