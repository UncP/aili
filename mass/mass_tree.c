/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "mass_tree.h"

mass_tree* new_mass_tree()
{
  mass_tree *mt = (mass_tree *)malloc(sizeof(mass_tree));

  mt->root = new_node(Border);

  return mt;
}

void free_mass_tree()
{
  // TODO
}
