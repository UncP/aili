CC=gcc
CFLAGS=-std=c11 -Wall -O3
DFLAGS=-DTest
AFLAGS=$(CC) $(CFLAGS) $(DFLAGS)

%.o: %.c
	$(AFLAGS) -c $^ -o $@

node_test: test/node_test.c src/node.o
	$(AFLAGS) -o $@ $^

clean:
	rm src/*.o
	rm *_test
