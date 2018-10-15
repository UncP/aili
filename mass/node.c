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

static inline uint32_t node_get_version(node *n)
{
  uint32_t version;
  __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

static inline interior_node* node_get_parent(node *n)
{
  uint32_t version = node_get_version(n);
  interior_node *parent;
  if (is_border(version)) {
    border_node *bn = (border_node *)n;
    __atomic_load(&bn->parent, &parent, __ATOMIC_ACQUIRE);
  } else {
    interior_node *in = (interior_node *)n;
    __atomic_load(&in->parent, &parent, __ATOMIC_ACQUIRE);
  }
  return parent;
}

static uint32_t node_get_stable_version(node *n)
{
  uint32_t version;
  do {
    version = node_get_version(n);
  } while (is_inserting(version) || is_spliting(version));
  return version;
}

// TODO: optimize
void node_lock(node *n)
{
  uint32_t version;
  uint32_t min, max = 128;
  while (1) {
    min = 4;
    while (1) {
      version = node_get_version(n);
      if (!is_locked(version))
        break;
      for (uint32_t i = 0; i != min; ++i)
        __asm__ __volatile__("pause" ::: "memory");
      if (min < max)
        min += min;
    }
    if (__atomic_compare_exchange_n(&n->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
}

// TODO: optimize
void node_unlock(node *n)
{
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  if (is_inserting(version)) {
    version = incr_vinsert(version);
    version = unset_insert(version);
  } else if (is_spliting(version)) {
    version = incr_vsplit(version);
    version = unset_split(version);
  }

  assert(__atomic_compare_exchange_n(&n->version, &version, unset_lock(version),
    0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

interior_node* node_get_locked_parent(node *n)
{
  interior_node *parent;
  while (1) {
    parent = node_get_parent(n);
    node_lock((node *)parent);
    if (node_get_parent(n) == parent)
      break;
    node_unlock((node *)parent);
  }
  return parent;
}

node* node_locate_child(node *n, const void *key, uint32_t len)
{

}
