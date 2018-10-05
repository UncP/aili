/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>
#include <time.h>
#include <stdlib.h>

#include "../palm/node.h"

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

  assert(n->keys == 10);
  node_validate(n);

  free_node(n);

  n = new_node(0, 0);

  // test random insert
  srand(time(NULL));
  for (uint32_t i = 0; i < 50; ++i) {
    key[rand() % len] = 'a' + (rand() % 26);
    assert(node_insert(n, key, len, (void *)(uint64_t)i) >= 0);
  }

  node_validate(n);

  free_node(n);
}

void test_node_insert_no_space()
{
  printf("test node insert no space\n");

  node *n = new_node(0, 0);

  // each key occupies `unit` bytes space
  uint32_t unit = 50 + key_byte + index_byte + value_bytes;
  // max keys a node can hold
  uint32_t max = (get_node_size() - (n->data - (char *)n)) / unit;
  key_buf(key, 50);
  char c = '0';
  for (uint32_t i = 0, j = len - 1; i < max; ++i) {
    if (c++ > '9') {
      c = '1';
      --j;
    }
    key[j] = c;
    assert(node_insert(n, key, len, (void *)(uint64_t)i) == 1);
    key[j] = '0';
  }

  key[0] = 'a';
  assert(node_insert(n, key, len, (void *)0) == -1);

  free_node(n);
}

void test_node_search()
{
  printf("test node search\n");

  node *n = new_node(Leaf, 0);

  key_buf(key, 50);

  srand(time(NULL));
  for (uint32_t i = 0; i < 50; ++i) {
    key[rand() % len] = '0' + (rand() % 10);
    int r = node_insert(n, key, len, (void *)(uint64_t)i);
    if (r == 1)
      assert((val_t)node_search(n, key, len) == i);
    else if (r == -1)
      assert(0);
  }

  node_validate(n);

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
    assert((val_t)node_descend(n, key, len) == i);
    key[len - i - 1] = '0';
  }

  key[0] = '3';
  assert((val_t)node_descend(n, key, len) == len);

  free_node(n);
}

void test_node_split_level_0()
{
  printf("test node split level 0\n");

  node *old = new_node(Leaf, 0); // level 0

  key_buf(key, 10);

  for (uint32_t i = 0; i < len; ++i) {
    key[len - i - 1] = '1';
    assert(node_insert(old, key, len, (void *)(uint64_t)(i+1)));
    key[len - i - 1] = '0';
  }

  node *new = new_node(Leaf, 0);

  char buf[len];
  uint32_t buf_len;
  node_split(old, new, buf, &buf_len);

  assert(old->keys == len / 2);
  assert(new->keys == len / 2);

  assert(old->next == new);
  assert(new->next == 0);

  node_validate(old);
  node_validate(new);

  node_print(old, 1);
  node_print(new, 1);

  free_node(old);
  free_node(new);
}

void test_node_split_level_1()
{
  printf("test node split level 1\n");

  node *old = new_node(Branch, 1); // level 1

  key_buf(key, 11);

  for (uint32_t i = 0; i < len; ++i) {
    key[len - i - 1] = '1';
    assert(node_insert(old, key, len, (void *)(uint64_t)(i+1)));
    key[len - i - 1] = '0';
  }

  node *new = new_node(Branch, 1);

  char buf[len];
  uint32_t buf_len;
  node_split(old, new, buf, &buf_len);

  assert(old->keys == len / 2);
  assert(new->keys == len / 2);

  assert(old->next == new);
  assert(new->next == 0);

  assert((uint64_t)new->first == (len / 2 + 1));

  node_validate(old);
  node_validate(new);

  free_node(old);
  free_node(new);
}

void test_print_node()
{
  printf("test print node\n");

  key_buf(key, 10);

  node *n = new_node(Leaf, 0);

  node_print(n, 1);

  for (uint32_t i = 0; i < len; ++i) {
    key[len - i - 1] = '1';
    assert(node_insert(n, key, len, (void *)(uint64_t)i) == 1);
    key[len - i - 1] = '0';
  }

  node_print(n, 0);

  node_print(n, 1);

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
  test_node_split_level_0();
  test_node_split_level_1();
  test_print_node();

  return 0;
}
