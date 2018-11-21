/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "blink_tree.h"

blink_tree* new_blink_tree(int thread_num)
{
  blink_tree *bt = (blink_tree *)malloc(sizeof(blink_tree));

  blink_node *root = new_blink_node(Root, 0);
  blink_node_insert_infinity_key(root);

  bt->root = root;

  bt->queue = 0;
  if (thread_num <= 0) {
    // queue is disabled
    return 0;
  }

  if (thread_num > 4)
    thread_num = 4;

  bt->queue = new_bounded_mapping_queue(max_key_size + sizeof(uint32_t) + 1 /* w or r */);
  return bt;
}

void free_blink_tree(blink_tree *bt)
{
  if (bt->queue)
    free_bounded_mapping_queue(bt->queue);

  // TODO: free all nodes
}

void blink_tree_flush(blink_tree *bt)
{
  if (bt->queue)
    bounded_mapping_queue_wait_empty(bt->queue);
}

struct stack {
  blink_node *path[max_descend_depth];
  uint32_t    depth;
};

static void blink_tree_root_split(blink_tree *bt, blink_node *left, const void *key, uint32_t len, blink_node *right)
{
  int level = blink_node_get_level(left);
  blink_node *new_root = new_blink_node(blink_node_get_type(left), level + 1);
  blink_node_insert_infinity_key(new_root);

  blink_node_set_first(new_root, left);
  blink_node_insert(new_root, key, len, right);

  int type = level ? Branch : Leaf;
  blink_node_set_type(left, type);
  blink_node_set_type(right, type);

  __atomic_store(&bt->root, &new_root, __ATOMIC_RELEASE);
}

static blink_node*
blink_tree_descend_to_leaf(blink_tree *bt, const void *key, uint32_t len, struct stack *stack, int is_write)
{
  blink_node *curr;
  stack->depth = 0;

  // acquire the latest root, it's ok to be stale if it changes right after
  __atomic_load(&bt->root, &curr, __ATOMIC_ACQUIRE);

  // no need to lock node since a node's level never changes
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
  void *v = (void *)val;

  for (;;) {
    switch (blink_node_insert(curr, k, len, v)) {
    case 0: { // key already exists
      assert(blink_node_get_level(curr) == 0);
      return 0;
    }
    case 1:
      // key insert succeed
      return 1;
    case -1: { // node needs to split
      // a normal split
      blink_node *new = new_blink_node(blink_node_get_type(curr), blink_node_get_level(curr));

      blink_node_split(curr, new, fkey, &flen);
      if (blink_node_is_before_key(curr, k, len))
        assert(blink_node_insert(new, k, len, v) == 1);
      else
        assert(blink_node_insert(curr, k, len, v) == 1);

      k = (void *)fkey; len = flen; v = (void *)new;

      // promote to parent
      if (stack.depth)
        curr = stack.path[--stack.depth];
      else {
        blink_tree_root_split(bt, curr, k, len, new);
        return 1;
      }
      break;
    }
    case -2: {
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
    case  0: return 0; // key not exists
    // move to right leaf
    case -1: {
      blink_node *next = blink_node_get_next(curr);
      blink_node_rlock(next);
      blink_node_unlock(curr);
      curr = next;
      break;
    }
    default:
      val = ret;
      return 1;
    }
  }
}
