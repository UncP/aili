/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <assert.h>

#include "../src/node.h"

#define key_buf(k, l)                \
  uint32_t len = l;                  \
  char k[len];                       \
  for (uint32_t i = 0; i < len; ++i) \
    k[i] = '0';                      \

void test_new_batch()
{
	printf("test new batch\n");
	batch *b = new_batch();

	assert(b->keys == 0);
	assert(b->off == 0);

	free_batch(b);
}

void test_batch()
{
	printf("test batch add\n");

	key_buf(key, 10);

	batch *b = new_batch();
	// test sequential insert
	for (uint32_t i = 0; i < 10; ++i) {
		key[len - i - 1] = '1';
		assert(batch_add(b, (i % 2) == 0 ? Write : Read, key, len, (void *)(uint64_t)i) == 1);
		key[len - i - 1] = '0';
	}

	key[3] = '1';
	assert(batch_add(b, Read, key, len, (void *)(uint64_t)6) == 1);
	key[3] = '0';

	assert(b->keys == 11);
	batch_validate(b);

	print_batch(b, 1);

	batch_clear(b);
	assert(b->keys == 0);
	assert(b->off == 0);

	// test random insert
	srand(time(NULL));
	for (uint32_t i = 0; i < 50; ++i) {
		int idx = rand() % len;
		key[idx] = '1';
		assert(batch_add(b, (i % 2) == 0 ? Write : Read, key, len, (void *)(uint64_t)i) == 1);
		key[idx] = '0';
	}

	assert(b->keys == 50);
	batch_validate(b);

	print_batch(b, 1);

	free_batch(b);
}

int main()
{
	test_new_batch();
	test_batch();

	return 0;
}
