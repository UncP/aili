CC=gcc
CFLAGS=-std=c99 -Wall -Werror -Wextra -O3
IFLAGS=-I./third_party
LFLAGS=./third_party/c_hashmap/libhashmap.a -lpthread
PFLAGS=-DLazy
TFLAGS=
MFLAGS=

PALMFLAGS=$(CC) $(CFLAGS) $(PFLAGS) $(IFLAGS) $(TFLAGS)
MASSFLAGS=$(CC) $(CFLAGS) $(MFLAGS) $(TFLAGS)

AILI_OBJ=palm/node.o palm/bounded_queue.o palm/worker.o palm/palm_tree.o palm/metric.o

default: lib

lib:$(AILI_OBJ)
	make third_party
	ar rcs libaili.a $(AILI_OBJ) third_party/c_hashmap/hashmap.o

test: node_test palm_batch_test palm_node_test palm_tree_test

palm/%.o: palm/%.c
	$(PALMFLAGS) -c $^ -o $@

palm_node_test: test/palm_node_test.c palm/node.o
	$(PALMFLAGS) -o $@ $^

palm_batch_test: test/palm_batch_test.c palm/node.o
	$(PALMFLAGS) -o $@ $^

palm_tree_test: test/palm_tree_test.c palm/node.o palm/worker.o palm/bounded_queue.o palm/palm_tree.o palm/metric.o
	$(PALMFLAGS) -o $@ $^ $(LFLAGS)

generate_data: ./generate_data.c
	$(CC) $(CFLAGS) -o $@ $^

mass/%.o: mass/%.c
	$(MASSFLAGS) -c $^ -o $@

node_test: test/mass_node_test.c mass/node.o
	$(MASSFLAGS) -o $@ $^ -lpthread

tree_test: test/mass_tree_test.c mass/node.o mass/mass_tree.o
	$(MASSFLAGS) -o $@ $^ -lpthread

third_party: third_party/c_hashmap
	cd third_party/c_hashmap && $(CC) $(CFLAGS) -c hashmap.c -o hashmap.o && ar rcs libhashmap.a hashmap.o

clean:
	rm palm/*.o mass/*.o *_test generate_data libaili.a; cd example && make clean
