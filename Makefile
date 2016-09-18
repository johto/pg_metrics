MODULE_big = pg_metrics
OBJS = pg_metrics.o

EXTENSION = pg_metrics
DATA = pg_metrics--1.0.sql

ifdef NO_PGXS
subdir = contrib/pg_metrics
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif
