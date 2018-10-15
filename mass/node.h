/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

// node type
#define Interior 0
#define Border   1

/**
 *   layout of a node's version (32 bit):
 *       lock   insert  split   delete   root   border  not-used    vinsert     vsplit
 *     |   1   |  1   |   1   |   1   |   1   |   1   |    2    |     8     |     16     |
 *
**/

#define LOCK_BIT   ((uint32_t)1 << 31)
#define INSERT_BIT ((uint32_t)1 << 30)
#define SPLIT_BIT  ((uint32_t)1 << 29)
#define DELETE_BIT ((uint32_t)1 << 28)
#define ROOT_BIT   ((uint32_t)1 << 27)
#define BORDER_BIT ((uint32_t)1 << 26)

// `vinsert` is a 8 bit field
#define get_vinsert(n)  ((uint32_t)(((n) >> 16) & 0xff))
#define incr_vinsert(n) ((n) + ((uint32_t)1 << 16))

// `vsplit` is a 16 bit field
#define get_vsplit(n)  ((n) & 0xffff)
#define incr_vsplit(n) ((n) + 1)

#define set_lock(n)   ((n) | LOCK_BIT)
#define set_insert(n) ((n) | INSERT_BIT)
#define set_split(n)  ((n) | SPLIT_BIT)
#define set_delete(n) ((n) | DELETE_BIT)
#define set_root(n)   ((n) | ROOT_BIT)
#define set_border(n) ((n) | BORDER_BIT)

#define unset_lock(n)   ((n) & (~LOCK_BIT))
#define unset_insert(n) ((n) & (~INSERT_BIT))
#define unset_split(n)  ((n) & (~SPLIT_BIT))

#define is_locked(n)    ((n) & LOCK_BIT)
#define is_inserting(n) ((n) & INSERT_BIT)
#define is_spliting(n)  ((n) & SPLIT_BIT)
#define is_deleted(n)   ((n) & DELETE_BIT)
#define is_root(n)      ((n) & ROOT_BIT)
#define is_border(n)    ((n) & BORDER_BIT)
#define is_interior(n)  (!is_border(n))

// see Mass Tree paper figure 2 for detail
typedef struct interior_node
{
  uint32_t version;

  uint32_t nkeys;
  uint64_t keyslice[15];

  void    *child[16];

  struct interior_node *parent;
}interior_node;

// see Mass Tree paper figure 2 for detail
typedef struct border_node
{
  uint32_t version;

  uint8_t  nremoved;
  uint8_t  keylen[15];
  uint64_t permutation;
  uint64_t keyslice[15];

  void *lv[15];

  struct border_node *prev;
  struct border_node *next;

  struct interior_node *parent;
}border_node;

// this is a little bit tricky, since both interior_node and border_node
// start with `version` field, we can convert them to `node` so that coding is easier
typedef struct node
{
  uint32_t version;
}node;

node* new_node(int type);
void free_node(node *n);
uint32_t node_stable_version(node *n);
void node_lock(node *n);
void node_unlock(node *n);

#endif /* _node_h_ */