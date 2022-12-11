# contrib/warmer/Makefile

EXTENSION = warmer
MODULE_big = warmer
DATA = warmer--1.1--1.2.sql warmer--1.1.sql warmer--1.0--1.1.sql
OBJS = warmer.o

PGFILEDESC = "warmer - preload relation data into system buffer cache"
PG_CONFIG ?= ../../build/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
