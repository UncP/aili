/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>

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

