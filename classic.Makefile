include Makefile.defs

OBJ_DIR = obj
TARGET = classic


CXXSRCS = \
	classic.cpp

include Makefile.rules

clean:
	rm $(OBJ_DIR) -rf
