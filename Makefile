MPICC=mpicc
CFLAGS=-Wall -Werror -g -O2
mpiqueue: mpiqueue.c
	$(MPICC) $(CFLAGS) -o $@ $<
