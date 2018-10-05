CC=gcc
CFLAGS=-std=c99 -Wall -Werror -Wextra -O3
DFLAGS=-DTest
IFLAGS=-I./third_party
LFLAGS=./third_party/c_hashmap/libhashmap.a -lpthread
AFLAGS=$(CC) $(CFLAGS) $(DFLAGS) $(IFLAGS)

AILI_OBJ=src/node.o src/bounded_queue.o src/worker.o src/palm_tree.o src/metric.o

%.o: %.c
	$(AFLAGS) -c $^ -o $@

default: lib

third_party: third_party/c_hashmap
	cd third_party/c_hashmap && $(CC) $(CFLAGS) -c hashmap.c -o hashmap.o && ar rcs libhashmap.a hashmap.o

lib:$(AILI_OBJ)
	make third_party
	ar rcs libaili.a $(AILI_OBJ) third_party/c_hashmap/hashmap.o

test: node_test batch_test barrier_test palm_tree_test

node_test: test/node_test.c src/node.o
	$(AFLAGS) -o $@ $^

batch_test: test/batch_test.c src/node.o
	$(AFLAGS) -o $@ $^

barrier_test: test/barrier_test.c src/barrier.o
	$(AFLAGS) -o $@ $^

palm_tree_test: test/palm_tree_test.c src/node.o src/worker.o src/bounded_queue.o src/palm_tree.o src/metric.o
	$(AFLAGS) -o $@ $^ $(LFLAGS)

generate_data: ./generate_data.c
	$(AFLAGS) -o $@ $^

clean:
	rm src/*.o *_test generate_data libaili.a
