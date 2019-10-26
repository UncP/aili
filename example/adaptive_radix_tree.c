/**
 *    author:     UncP
 *    date:    2019-10-18
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>

#include "../art/art.h"

/* a simple example as how to use adaptive radix tree */

int main()
{
  char hello[6];
  hello[0] = 5;
  hello[1] = 'h';
  hello[2] = 'e';
  hello[3] = 'l';
  hello[4] = 'l';
  hello[5] = 'o';

  adaptive_radix_tree *art = new_adaptive_radix_tree();

  // adaptive radix tree is just a toy, value is not stored
  adaptive_radix_tree_put(art, (const void *)&hello[1], 5);
  assert(adaptive_radix_tree_get(art, (const void *)&hello[1], 5) == (void *)&hello[1]);
  printf("%s\n", hello);

  free_adaptive_radix_tree(art);
  return 0;
}
