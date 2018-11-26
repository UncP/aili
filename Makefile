CC=gcc
CFLAGS=-std=c99 -Wall -Werror -Wextra -O3
IFLAGS=-I./third_party
LFLAGS=./third_party/c_hashmap/libhashmap.a -lpthread
PFLAGS=-DLazy -DPrefix
DFLAGS=
BFLAGS=
MFLAGS=

PALMFLAGS=$(CC) $(CFLAGS) $(PFLAGS) $(IFLAGS) $(DFLAGS)
BLINKFLAGS=$(CC) $(CFLAGS) $(BFLAGS) $(DFLAGS)
MASSFLAGS=$(CC) $(CFLAGS) $(MFLAGS) $(DFLAGS)

PALM_OBJ=palm/node.o palm/bounded_queue.o palm/worker.o palm/palm_tree.o palm/metric.o palm/allocator.o
BLINK_OBJ=palm/node.o palm/allocator.o blink/node.o blink/blink_tree.o blink/mapping_array.o

default: lib

lib:$(PALM_OBJ) $(BLINK_OBJ)
	make third_party
	ar rcs libaili.a $(PALM_OBJ) $(BLINK_OBJ) third_party/c_hashmap/hashmap.o

test: node_test palm_batch_test palm_node_test palm_tree_test

palm/%.o: palm/%.c
	$(PALMFLAGS) -c $^ -o $@

palm_node_test: test/palm_node_test.c palm/node.o
	$(PALMFLAGS) -o $@ $^

palm_batch_test: test/palm_batch_test.c palm/node.o
	$(PALMFLAGS) -o $@ $^

palm_tree_test: test/palm_tree_test.c palm/node.o palm/worker.o palm/bounded_queue.o palm/palm_tree.o \
	palm/metric.o palm/allocator.o
	$(PALMFLAGS) -o $@ $^ $(LFLAGS)

generate_data: ./generate_data.c
	$(CC) $(CFLAGS) -o $@ $^

blink/%.o: blink/%.c
	$(BLINKFLAGS) -c $^ -o $@

blink_tree_test: test/blink_tree_test.c blink/node.o blink/blink_tree.o blink/mapping_array.o palm/node.o \
	palm/allocator.o
	$(BLINKFLAGS) -o $@ $^ -lpthread

mass/%.o: mass/%.c
	$(MASSFLAGS) -c $^ -o $@

node_test: test/mass_node_test.c mass/node.o
	$(MASSFLAGS) -o $@ $^ -lpthread

tree_test: test/mass_tree_test.c mass/node.o mass/mass_tree.o
	$(MASSFLAGS) -o $@ $^ -lpthread

third_party: third_party/c_hashmap
	cd third_party/c_hashmap && $(CC) $(CFLAGS) -c hashmap.c -o hashmap.o && ar rcs libhashmap.a hashmap.o

clean:
	rm palm/*.o blink/*.o mass/*.o *_test generate_data libaili.a; cd example && make clean
