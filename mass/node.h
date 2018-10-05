/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

typedef struct interior_node
{
  uint32_t version;

  uint32_t nkeys;
  uint64_t keys[15];

  void    *child[16];

  struct interior_node *parent;
}interior_node;

typedef struct border_node
{
  uint32_t version;

  uint8_t  nkeys;
  uint8_t  keylen[15];
  uint64_t permutation;
  uint64_t keys[15];

  void *lv[15];

  struct border_node *prev;
  struct border_node *next;

  interior_node *parent;
}border_node;

#endif /* _node_h_ */