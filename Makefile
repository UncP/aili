CC=gcc
CFLAGS=-std=c11 -Wall -O3
DFLAGS=-DTest
AFLAGS=$(CC) $(CFLAGS) $(DFLAGS)

%.o: %.c
	$(AFLAGS) -c $^ -o $@

all: node_test batch_test barrier_test

node_test: test/node_test.c src/node.o
	$(AFLAGS) -o $@ $^

batch_test: test/batch_test.c src/node.o
	$(AFLAGS) -o $@ $^

barrier_test: test/barrier_test.c src/barrier.o
	$(AFLAGS) -o $@ $^

clean:
	rm src/*.o
	rm *_test
