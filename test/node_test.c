/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>

#include "../src/node.h"

#define key_buf(k, l)                \
	uint32_t len = l;                  \
	char k[len];                       \
	for (uint32_t i = 0; i < len; ++i) \
		k[i] = '0';                      \

void test_set_node_size()
{
	printf("test set node size\n");

	set_node_size(3190);
	assert(get_node_size() == node_min_size);
	set_node_size(1024 * 1024 * 2);
	assert(get_node_size() == node_max_size);
	set_node_size(10000);
	assert(get_node_size() == 8192);

	set_node_size(node_min_size);
}

void test_new_node()
{
	printf("test new node\n");
	node *n = new_node(Leaf, 1);

	assert(n->type == Leaf);
	assert(n->level == 1);
	assert(n->keys == 0);
	assert(n->pre == 0);
	assert(n->off == 0);
	assert(n->next == 0);
	assert(n->first == 0);

	free_node(n);
}

void test_node_insert()
{
	printf("test node insert\n");

	key_buf(key, 10);

	node *n = new_node(0, 0);
	// test sequential insert
	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '1';
		assert(node_insert(n, key, len, (void *)(uint64_t)i) == 1);
		key[len - i - 1] = '0';
	}

	assert(node_get_key_count(n) == 10);
	node_validate(n);

	free_node(n);

	n = new_node(0, 0);

	// test random insert
	srand(time(NULL));
	for (uint32_t i = 0; i < len; ++i) {
		key[rand() % len] = 'a' + (rand() % len);
		assert(node_insert(n, key, len, (void *)(uint64_t)i) >= 0);
	}

	node_validate(n);

	free_node(n);
}

void test_node_insert_no_space()
{
	printf("test node insert no space\n");

	node *n = new_node(0, 0);

	// each key occupies 89 + 1 + 2 + 8 = 100 bytes
	// so the 41st key will not fit in a 4096 bytes size node
	key_buf(key, 89);
	for (uint32_t i = 0; i < 40; ++i) {
		key[len - i - 1] = '1';
		assert(node_insert(n, key, len, (void *)(uint64_t)i) == 1);
		key[len - i - 1] = '0';
	}

	key[0] = '1';
	assert(node_insert(n, key, len, (void *)0) == -1);

	free_node(n);
}

void test_node_search()
{
	printf("test node search\n");

	node *n = new_node(Leaf, 0);

	key_buf(key, 10);

	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '1';
		assert(node_insert(n, key, len, (void *)(uint64_t)i));
		key[len - i - 1] = '0';
	}

	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '1';
		void *v = node_search(n, key, len);
		assert((uint64_t)v == i);
		key[len - i - 1] = '0';
	}

	free_node(n);
}

void test_node_descend()
{
	printf("test node descend\n");

	node *n = new_node(Branch, 1);

	key_buf(key, 10);

	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '2';
		assert(node_insert(n, key, len, (void *)(uint64_t)(i+1)));
		key[len - i - 1] = '0';
	}

	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '1';
		assert((uint64_t)node_descend(n, key, len) == i);
		key[len - i - 1] = '0';
	}

	key[0] = '3';
	assert((uint64_t)node_descend(n, key, len) == len);

	free_node(n);
}

void test_print_node()
{
	printf("test print node\n");

	key_buf(key, 10);

	node *n = new_node(Leaf, 1);

	print_node(n, 1);

	for (uint32_t i = 0; i < len; ++i) {
		key[len - i - 1] = '1';
		assert(node_insert(n, key, len, (void *)(uint64_t)i) == 1);
		key[len - i - 1] = '0';
	}

	print_node(n, 0);

	print_node(n, 1);

	free_node(n);
}

int main()
{
	test_set_node_size();
	test_new_node();
	test_node_insert();
	test_node_insert_no_space();
	test_node_search();
	test_node_descend();
	test_print_node();

	return 0;
}
