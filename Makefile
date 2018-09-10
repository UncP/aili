CC=gcc
CFLAGS=-std=c99 -Wall -O3
DFLAGS=-DTest
AFLAGS=$(CC) $(CFLAGS)

%.o: %.c
	$(AFLAGS) -c $^ -o $@ $(DFLAGS)

all: node_test batch_test barrier_test

node_test: test/node_test.c src/node.o
	$(AFLAGS) -o $@ $^ $(DFLAGS)

batch_test: test/batch_test.c src/node.o
	$(AFLAGS) -o $@ $^ $(DFLAGS)

barrier_test: test/barrier_test.c src/barrier.o
	$(AFLAGS) -o $@ $^ $(DFLAGS)

palm_tree_test: test/palm_tree_test.c src/barrier.o src/node.o src/worker.o src/palm_tree.o
	$(AFLAGS) -o $@ $^ $(DFLAGS)

gen_data: ./test_data.c
	$(AFLAGS) -o $@ $^

clean:
	rm src/*.o *_test gen_data