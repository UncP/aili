CC=gcc
CFLAGS=-std=c99 -D_GNU_SOURCE -Wall -Werror -Wextra -O3 -fno-strict-aliasing
IFLAGS=-I./third_party
LFLAGS=./third_party/c_hashmap/libhashmap.a -lpthread -lm
PFLAGS=-DLazy #-DPrefix -DBStar
DFLAGS=
BFLAGS=
MFLAGS=-DTest
AFLAGS=-DTest
HFLAGS=-DTest

PALMFLAGS=$(CC) $(CFLAGS) $(PFLAGS) $(IFLAGS) $(DFLAGS)
BLINKFLAGS=$(CC) $(CFLAGS) $(BFLAGS) $(DFLAGS)
MASSFLAGS=$(CC) $(CFLAGS) $(MFLAGS) $(DFLAGS)
ARTFLAGS=$(CC) $(CFLAGS) $(AFLAGS) $(DFLAGS)
HOTFLAGS=$(CC) $(CFLAGS) $(HFLAGS) $(DFLAGS)
ONEFLAGS=$(CC) $(CFLAGS) $(DFLAGS) $(LFLAGS)

PALM_OBJ=palm/node.o palm/bounded_queue.o palm/worker.o palm/palm_tree.o palm/metric.o palm/allocator.o
BLINK_OBJ=palm/node.o palm/allocator.o blink/node.o blink/blink_tree.o blink/mapping_array.o
MASS_OBJ=mass/mass_node.o mass/mass_tree.o
ART_OBJ=art/art_node.o art/art.o
HOT_OBJ=hot/hot_node.o hot/hot.o

default: lib

lib:$(PALM_OBJ) $(BLINK_OBJ) $(MASS_OBJ) $(ART_OBJ)
	make third_party
	ar rcs libaili.a $(PALM_OBJ) $(BLINK_OBJ) $(MASS_OBJ) $(ART_OBJ) third_party/c_hashmap/hashmap.o

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

generate_data: generate_data.c
	$(CC) $(CFLAGS) -o $@ $^ -lpthread

blink/%.o: blink/%.c
	$(BLINKFLAGS) -c $^ -o $@

blink_tree_test: test/blink_tree_test.c blink/node.o blink/blink_tree.o blink/mapping_array.o palm/node.o \
	palm/allocator.o
	$(BLINKFLAGS) -o $@ $^ -lpthread

mass/%.o: mass/%.c
	$(MASSFLAGS) -c $^ -o $@

mass_node_test: test/mass_node_test.c mass/mass_node.o
	$(MASSFLAGS) -o $@ $^ -lpthread

mass_tree_test: test/mass_tree_test.c mass/mass_node.o mass/mass_tree.o palm/allocator.o
	$(MASSFLAGS) -o $@ $^ -lpthread

art/%.o: art/%.c
	$(ARTFLAGS) -c $^ -o $@

art_test: test/art_test.c art/art_node.o art/art.o palm/allocator.o
	$(ARTFLAGS) -o $@ $^ -lpthread

hot/%.o: hot/%.c
	$(HOTFLAGS) -c $^ -o $@

util/%.o: util/%.c
	$(ONEFLAGS) -c $^ -o $@

one_test: test/one_test.c util/rng.o $(PALM_OBJ) $(BLINK_OBJ) $(MASS_OBJ) $(ART_OBJ) $(HOT_OBJ)
	$(ONEFLAGS) -o $@ $^ $(LFLAGS)

third_party: third_party/c_hashmap/hashmap.o
	cd third_party/c_hashmap && $(CC) $(CFLAGS) -c hashmap.c -o hashmap.o && ar rcs libhashmap.a hashmap.o

clean:
	rm palm/*.o blink/*.o mass/*.o art/*.o util/*.o *_test generate_data libaili.a; cd example && make clean
