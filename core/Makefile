# Cross-check valuegen.py/valuegen.c and dataset.py/dataset.c against
# each other.
#   make -C core cross-check          # both
#   make -C core valuegen-cross-check
#   make -C core dataset-cross-check
# Each target runs the matching *_cross_check.py, which builds and drives
# the C side itself. See design/value-generation.md, valuegen_cross_check.py's
# and dataset_cross_check.py's docstrings.
CC      ?= cc
CFLAGS  ?= -O2 -Wall
PYTHON  ?= python3
JANSSON_SO ?= /usr/lib/x86_64-linux-gnu/libjansson.so.4

.PHONY: cross-check valuegen-cross-check dataset-cross-check clean

cross-check: valuegen-cross-check dataset-cross-check

valuegen-cross-check:
	$(PYTHON) -I valuegen_cross_check.py

dataset-cross-check:
	$(PYTHON) -I dataset_cross_check.py

# The *_cross_check.py scripts build their C binary themselves (in a
# temp dir) on each run; these targets are only for building by hand,
# e.g. to run under a debugger.
valuegen_cross_check: valuegen_cross_check.c valuegen.c valuegen.h
	$(CC) $(CFLAGS) -I. -o $@ valuegen_cross_check.c valuegen.c

dataset_cross_check: dataset_cross_check.c dataset.c valuegen.c dataset.h valuegen.h
	$(CC) $(CFLAGS) -I. -o $@ \
		dataset_cross_check.c dataset.c valuegen.c $(JANSSON_SO)

# The C ingest engine with an adapter that stores nothing: measures the
# engine itself. Run as: DATASET=... ./ingest_null --writer null --output F --node-index 0
ingest_null: ingest_null.c ingest.c ingest.h dataset.c valuegen.c dataset.h valuegen.h
	$(CC) $(CFLAGS) -I. -o $@ ingest_null.c ingest.c dataset.c valuegen.c $(JANSSON_SO) -lm

clean:
	rm -f valuegen_cross_check dataset_cross_check ingest_null
