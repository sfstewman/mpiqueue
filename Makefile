MPICC=mpicc
CFLAGS=-Wall -Werror -g -O2
REV_SHA=$(shell git rev-parse --short HEAD)

mpiqueue: mpiqueue.c
	$(MPICC) $(CFLAGS) -DREV_SHA="\"$(REV_SHA)\"" -o $@ $<

all: mpiqueue
