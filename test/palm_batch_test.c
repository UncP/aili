/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "../palm/node.h"

#define key_buf(k, l)                \
  uint32_t len = l;                  \
  char k[len];                       \
  for (uint32_t i = 0; i < len; ++i) \
    k[i] = '0';                      \

void test_set_batch_size()
{
  printf("test set batch size\n");

  set_batch_size(node_min_size + 1);
  assert(get_batch_size() == node_min_size);
  set_batch_size(node_max_size + 1);
  assert(get_batch_size() == node_max_size);
}

void test_new_batch()
{
  printf("test new batch\n");
  batch *b = new_batch();

  assert(b->keys == 0);
  assert(b->off == 0);

  free_batch(b);
}

void test_batch_clear()
{
  printf("test batch clear\n");

  key_buf(key, 10);

  batch *b = new_batch();

  assert(batch_add_write(b, key, len, (void *)0) == 1);

  batch_clear(b);

  assert(b->keys == 0);
  assert(b->off == 0);

  free_batch(b);
}

void test_batch_write()
{
  printf("test batch write\n");

  key_buf(key, 10);

  batch *b = new_batch();

  // test sequential insert
  for (uint32_t i = 0; i < 10; ++i) {
    key[len - i - 1] = '1';
    if ((i % 2) == 0)
      assert(batch_add_write(b, key, len, (void *)(uint64_t)i) == 1);
    else
      assert(batch_add_read(b, key, len) == 1);
    key[len - i - 1] = '0';
  }

  key[3] = '1';
  assert(batch_add_read(b, key, len) == 1);
  key[3] = '0';

  assert(b->keys == 11);
  batch_validate(b);

  free_batch(b);

  b = new_batch();

  // test random insert
  srand(time(NULL));
  for (uint32_t i = 0; i < 50; ++i) {
    int idx = rand() % len;
    key[idx] = '1';
    if ((i % 2) == 0)
      assert(batch_add_write(b, key, len, (void *)(uint64_t)i) == 1);
    else
      assert(batch_add_read(b, key, len) == 1);
    key[idx] = '0';
  }

  assert(b->keys == 50);
  batch_validate(b);

  free_batch(b);
}

void test_batch_read()
{
  printf("test batch read\n");

  key_buf(key, 10);

  batch *b = new_batch();

  // test sequential insert
  for (uint32_t i = 0; i < 10; ++i) {
    key[len - i - 1] = '1';
    if ((i % 2) == 0)
      assert(batch_add_write(b, key, len, (void *)(uint64_t)i) == 1);
    else
      assert(batch_add_read(b, key, len) == 1);
    key[len - i - 1] = '0';
  }

  uint32_t op;
  char *key2;
  uint32_t len2;
  void *val;
  for (uint32_t i = 0; i < 10; ++i) {
    key[len - i - 1] = '1';
    batch_read_at(b, i, &op, (void **)&key2, &len2, &val);
    assert((i % 2 == 0) ? op == Write : op == Read);
    assert(compare_key(key, len, key2, len2) == 0);
    assert((i % 2 == 0) ? (*(val_t *)val == i) : (*(val_t *)val == 0));
    key[len - i - 1] = '0';
  }

  free_batch(b);
}

void test_print_batch()
{
  printf("test print batch\n");

  key_buf(key, 10);

  batch *b = new_batch();
  // test random insert
  srand(time(NULL));
  for (uint32_t i = 0; i < 20; ++i) {
    int idx = rand() % len;
    key[idx] = '1';
    if ((i % 2) == 0)
      assert(batch_add_write(b, key, len, (void *)(uint64_t)i) == 1);
    else
      assert(batch_add_read(b, key, len) == 1);
    key[idx] = '0';
  }

  batch_print(b, 0);
  batch_print(b, 1);

  free_batch(b);
}

int main()
{
  test_set_batch_size();
  test_new_batch();
  test_batch_clear();
  test_batch_write();
  test_batch_read();
  test_print_batch();

  return 0;
}
