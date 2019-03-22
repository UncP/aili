/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>
// TODO: remove this
#include <stdio.h>

#include "../palm/allocator.h"
#include "blink_tree.h"

static void* run(void *arg)
{
  blink_tree *bt = (blink_tree *)arg;
  mapping_array *q = bt->array;

  int   idx;
  void *tmp;
  while (1) {
    char *buf = (char *)mapping_array_get_busy(q, &idx);

    if (unlikely(buf == 0))
      break;

    int is_write = (int)(buf[0]);
    uint32_t len = *((uint32_t *)(buf + 1));
    const void *key = (void *)(buf + 5);
    const void *val = (void *)(*((uint64_t *)(buf + len + 5)));
    if (is_write) {
      blink_tree_write(bt, key, len, val);
    } else {
      #ifdef Test
        assert(blink_tree_read(bt, key, len, &tmp));
        assert((uint64_t)tmp == 3190);
      #else
        blink_tree_read(bt, key, len, &tmp);
      #endif
    }

    mapping_array_put_busy(q, idx);
  }

  return (void *)0;
}

blink_tree* new_blink_tree(int thread_num)
{
#ifdef Allocator
  init_allocator();
#endif

  blink_tree *bt = (blink_tree *)malloc(sizeof(blink_tree));

  blink_node *root = new_blink_node(Root, 0);

  uint32_t offset = (char *)(&(root->pn)) - (char *)(&(root->lock));
  set_node_offset(offset);

  blink_node_insert_infinity_key(root);

  bt->root = root;

  bt->array = 0;
  if (thread_num <= 0) {
    // array is disabled
    return bt;
  }

  if (thread_num > 4)
    thread_num = 4;

  bt->array = new_mapping_array(1 /* w or r */ + sizeof(uint32_t) + max_key_size + sizeof(void *));

  bt->thread_num = thread_num;
  bt->ids = (pthread_t *)malloc(bt->thread_num * sizeof(pthread_t));

  for (int i = 0; i < bt->thread_num; ++i)
    assert(pthread_create(&bt->ids[i], 0, run, (void *)bt) == 0);

  return bt;
}

void free_blink_tree(blink_tree *bt)
{
  if (bt->array) {
    free_mapping_array(bt->array);

    for (int i = 0; i < bt->thread_num; ++i)
      assert(pthread_join(bt->ids[i], 0) == 0);
    free((void *)bt->ids);
  }

  // TODO: free all nodes

  free((void *)bt);
}

void blink_tree_flush(blink_tree *bt)
{
  if (bt->array)
    mapping_array_wait_empty(bt->array);
}

void blink_tree_schedule(blink_tree *bt, int is_write, const void *key, uint32_t len, const void *val)
{
  assert(bt->array);

  int idx;
  char *buf = (char *)mapping_array_get_free(bt->array, &idx);

  buf[0] = (char)is_write;
  *((uint32_t *)(buf + 1)) = len;
  memcpy(buf + 5, key, len);
  if (val)
    *((uint64_t *)(buf + 5 + len)) = *((uint64_t *)&val);
  else
    *((uint64_t *)(buf + 5 + len)) = 0;

  mapping_array_put_free(bt->array, idx);
}

struct stack {
  blink_node *path[max_descend_depth];
  uint32_t    depth;
};

static void blink_tree_root_split(blink_tree *bt, blink_node *left, const void *key, uint32_t len, blink_node *right)
{
  assert(blink_node_is_root(left));

  int level = blink_node_get_level(left);
  blink_node *new_root = new_blink_node(blink_node_get_type(left), level + 1);

  blink_node_insert_infinity_key(new_root);

  blink_node_set_first(new_root, left);
  assert(blink_node_insert(new_root, key, len, (const void *)right) == 1);

  int type = level ? Branch : Leaf;
  blink_node_set_type(left, type);
  blink_node_set_type(right, type);

  // it's ok to use `relaxed` operation, but it doesn't matter
  __atomic_store(&bt->root, &new_root, __ATOMIC_RELEASE);
}

static blink_node*
blink_tree_descend_to_leaf(blink_tree *bt, const void *key, uint32_t len, struct stack *stack, int is_write)
{
  blink_node *curr;
  stack->depth = 0;

  // acquire the latest root, it's ok to be stale if it changes right after
  // actually it's also ok to use `relaxed` operation
  __atomic_load(&bt->root, &curr, __ATOMIC_ACQUIRE);

  // we can read `level` without lock this node since a node's level never changes
  int level = blink_node_get_level(curr);

  while (level) {
    assert(curr);
    blink_node_rlock(curr);
    blink_node *child = blink_node_descend(curr, key, len);
    blink_node_unlock(curr);
    if (likely(blink_node_get_level(child) != level)) {
      stack->path[stack->depth++] = curr;
      --level;
    }
    curr = child;
  }

  assert(curr && blink_node_get_level(curr) == 0);
  if (is_write)
    blink_node_wlock(curr);
  else
    blink_node_rlock(curr);

  return curr;
}

// Reference: Efficient Locking for Concurrent Operations on B-Trees
int blink_tree_write(blink_tree *bt, const void *key, uint32_t len, const void *val)
{
  struct stack stack;
  blink_node *curr = blink_tree_descend_to_leaf(bt, key, len, &stack, 1 /* is_write */);

  char fkey[max_key_size];
  uint32_t flen;
  void *k = (void *)key;
  uint32_t l = len;
  void *v = (void *)val;

  for (;;) {
    switch (blink_node_insert(curr, k, l, v)) {
    case 0: { // key already exists
      assert(blink_node_get_level(curr) == 0);
      blink_node_unlock(curr);
      return 0;
    }
    case 1:
      // key insert succeed
      blink_node_unlock(curr);
      return 1;
    case -1: { // node needs to split
      // a normal split
      blink_node *new = new_blink_node(blink_node_get_type(curr), blink_node_get_level(curr));

      blink_node_split(curr, new, fkey, &flen);
      if (blink_node_need_move_right(curr, k, l))
        assert(blink_node_insert(new, k, l, v) == 1);
      else
        assert(blink_node_insert(curr, k, l, v) == 1);

      memcpy(k, fkey, flen); l = flen; v = (void *)new;

      // promote to parent
      if (stack.depth) {
        blink_node *parent = stack.path[--stack.depth];
        // we can unlock `curr` first, but to be safe just lock `parent` first
        blink_node_wlock(parent);
        blink_node_unlock(curr);
        curr = parent;
      } else {
        blink_tree_root_split(bt, curr, k, len, new);
        blink_node_unlock(curr);
        return 1;
      }
      break;
    }
    case -3: {
      // need to move to right
      blink_node *next = blink_node_get_next(curr);
      blink_node_wlock(next);
      blink_node_unlock(curr);
      curr = next;
      break;
    }
    default: assert(0);
    }
  }
}

// Reference: Efficient Locking for Concurrent Operations on B-Trees
int blink_tree_read(blink_tree *bt, const void *key, uint32_t len, void **val)
{
  struct stack stack;
  blink_node *curr = blink_tree_descend_to_leaf(bt, key, len, &stack, 0 /* is_write */);

  void *ret;
  for (;;) {
    switch ((int64_t)(ret = blink_node_search(curr, key, len))) {
    case  0: { // key not exists
      blink_node_unlock(curr);
      *val = 0;
      return 0;
    }
    // move to right leaf
    case -1: {
      blink_node *next = blink_node_get_next(curr);
      blink_node_rlock(next);
      blink_node_unlock(curr);
      curr = next;
      break;
    }
    default:
      blink_node_unlock(curr);
      *val = ret;
      return 1;
    }
  }
}
