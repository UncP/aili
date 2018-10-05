all:palm_tree.c
	cd .. && make lib
	gcc -std=c99 -o palm_tree palm_tree.c ../libaili.a ../third_party/c_hashmap/libhashmap.a

clean:
	rm palm_tree
