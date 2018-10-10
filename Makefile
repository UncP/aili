CC=gcc
CFLAGS=-std=c99 -Wall -Werror -Wextra -O3
DFLAGS=-DTest
IFLAGS=-I./third_party
LFLAGS=./third_party/c_hashmap/libhashmap.a -lpthread
AFLAGS=$(CC) $(CFLAGS) $(DFLAGS) $(IFLAGS)

AILI_OBJ=palm/node.o palm/bounded_queue.o palm/worker.o palm/palm_tree.o palm/metric.o

%.o: %.c
	$(AFLAGS) -c $^ -o $@

default: lib

third_party: third_party/c_hashmap
	cd third_party/c_hashmap && $(CC) $(CFLAGS) -c hashmap.c -o hashmap.o && ar rcs libhashmap.a hashmap.o

lib:$(AILI_OBJ)
	make third_party
	ar rcs libaili.a $(AILI_OBJ) third_party/c_hashmap/hashmap.o

test: node_test batch_test barrier_test palm_tree_test

palm_node_test: test/palm_node_test.c palm/node.o
	$(AFLAGS) -o $@ $^

palm_batch_test: test/palm_batch_test.c palm/node.o
	$(AFLAGS) -o $@ $^

palm_tree_test: test/palm_tree_test.c palm/node.o palm/worker.o palm/bounded_queue.o palm/palm_tree.o palm/metric.o
	$(AFLAGS) -o $@ $^ $(LFLAGS)

generate_data: ./generate_data.c
	$(AFLAGS) -o $@ $^

mass_node_test: test/mass_node_test.c mass/node.o
	$(AFLAGS) -o $@ $^ -lpthread

clean:
	rm palm/*.o mass/*.o *_test generate_data libaili.a; cd example && make clean
