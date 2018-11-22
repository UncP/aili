all:palm_tree.c blink_tree.c
	cd .. && make lib
	gcc -std=c99 -o palm_tree palm_tree.c ../libaili.a
	gcc -std=c99 -o blink_tree blink_tree.c ../libaili.a

clean:
	rm palm_tree
	rm blink_tree
