/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "node.h"

interior_node* new_interior_node()
{
  interior_node in = (interior_node *)malloc(sizeof(interior_node));

  in->version = 0;
  in->nkeys   = 0;
  in->parent  = 0;

  return in;
}

border_node* new_border_node()
{
  border_node *bn = (border_node *)malloc(sizeof(border_node));

  bn->version = 0;

  bn->nkeys = 0;

  bn->permutation = 0;

  bn->prev = 0;
  bn->next = 0;

  bn->parent = 0;

  return bn;
}
