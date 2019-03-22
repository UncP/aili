/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

#include "../palm/allocator.h"
#include "node.h"

blink_node *new_blink_node(uint8_t type, uint8_t level)
{
#ifdef Allocator
  blink_node *bn = (blink_node *)allocator_alloc(get_node_size());
#else
  blink_node *bn = (blink_node *)malloc(get_node_size());
#endif

  latch_init(bn->lock);
  node_init(bn->pn, type | Blink, level);

  return bn;
}

void free_blink_node(blink_node *bn)
{
#ifdef Allocator
  allocator_free((void *)bn);
#else
  free((void *)bn);
#endif
}

void free_blink_tree_node(blink_node *bn)
{
  (void)bn;
  // TODO
}

inline void blink_node_rlock(blink_node *bn)
{
  latch_rlock(bn->lock);
}

inline void blink_node_wlock(blink_node *bn)
{
  latch_wlock(bn->lock);
}

inline void blink_node_unlock(blink_node *bn)
{
  latch_unlock(bn->lock);
}

blink_node* blink_node_descend(blink_node *bn, const void *key, uint32_t len)
{
  return (blink_node *)node_descend(bn->pn, key, len);
}

int blink_node_insert(blink_node *bn, const void *key, uint32_t len, const void *val)
{
  return node_insert(bn->pn, key, len, val);
}

void* blink_node_search(blink_node *bn, const void *key, uint32_t len)
{
  return node_search(bn->pn, key, len);
}

void blink_node_split(blink_node *old, blink_node *new, char *pkey, uint32_t *plen)
{
  node_split(old->pn, new->pn, pkey, plen);
  node_insert_fence(old->pn, new->pn, (void *)new, pkey, plen);
}

int blink_node_need_move_right(blink_node *bn, const void *key, uint32_t len)
{
  return node_need_move_right(bn->pn, key, len);
}

void blink_node_insert_infinity_key(blink_node *bn)
{
  char key[max_key_size];
  memset(key, 0xff, max_key_size);
  assert(blink_node_insert(bn, key, max_key_size, 0) == 1);
}

#ifdef Test

void blink_node_print(blink_node *bn, int detail)
{
  node_print(bn->pn, detail);
}

#endif /* Test */
