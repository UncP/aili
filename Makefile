CFLAGS=gcc -std=c99 -Wall -O3
TFLAGS=-lpthread

node_test: test/node.c src/node.o
	$(CFLAGS) -o $@ $^

clean:
	rm src/*.o
	rm *_test
