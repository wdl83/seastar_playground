include Makefile.defs
include Makefile.seastar

OBJ_DIR = obj_$(BUILD)
TARGET = parallel

CXXSRCS += parallel.cpp

include Makefile.rules

clean:
	rm $(OBJ_DIR) -rf
