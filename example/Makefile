all:palm_tree.c blink_tree.c mass_tree.c adaptive_radix_tree.c
	cd .. && make lib "DFLAGS+=-DAllocator"
	gcc -std=c99 -o palm_tree palm_tree.c ../libaili.a -lpthread -lm
	gcc -std=c99 -o blink_tree blink_tree.c ../libaili.a -lpthread -lm -D_GNU_SOURCE
	gcc -std=c99 -o mass_tree mass_tree.c ../libaili.a -lpthread -lm
	gcc -std=c99 -o adaptive_radix_tree adaptive_radix_tree.c ../libaili.a -lpthread -lm

clean:
	rm palm_tree
	rm blink_tree
	rm mass_tree
	rm adaptive_radix_tree
