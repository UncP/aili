/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <assert.h>

#include "node.h"

static interior_node* new_interior_node()
{
  interior_node *in = (interior_node *)malloc(sizeof(interior_node));

  in->version = 0;

  in->nkeys   = 0;
  in->parent  = 0;

  return in;
}

static border_node* new_border_node()
{
  border_node *bn = (border_node *)malloc(sizeof(border_node));

  uint32_t version = 0;

  bn->version = set_border(version);

  bn->nremoved = 0;

  bn->permutation = 0;

  bn->prev = 0;
  bn->next = 0;

  bn->parent = 0;

  return bn;
}

node* new_node(int type)
{
  return likely(type == Border) ? (node *)new_border_node() : (node *)new_interior_node();
}

void free_node(node *n)
{
  // node type does not matter
  free((void *)n);
}

uint32_t node_stable_version(node *n)
{
  uint32_t version;
  do {
    __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  } while (is_inserting(version) || is_spliting(version));
  return version;
}

void node_lock(node *n)
{
  uint32_t version;
  uint32_t min, max = 128;
  do {
    min = 4;
    again:
    __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
    if (is_locked(version)) {
      for (uint32_t i = 0; i != min; ++i)
        __asm__ __volatile__("pause" ::: "memory");
      if (min < max) min += min;
      goto again;
    }
  } while (!__atomic_compare_exchange_n(&n->version, &version, set_lock(version),
      1, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
}

void node_unlock(node *n)
{
  uint32_t version;
  __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  assert(is_locked(version));
  assert(__atomic_compare_exchange_n(&n->version, &version, unset_lock(version),
      0, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED));
}
