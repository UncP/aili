/**
 *    author:     UncP
 *    date:    2018-12-14
 *    license:    BSD-3
**/

#include <assert.h>

#include "../mass/mass_tree.h"

/* a simple example as how to use mass tree */

int main()
{
  static const char *hello = "hello";

  mass_tree *mt = new_mass_tree(2 /* thread_number */);

  // since mass tree is just a toy, i didn't intend to use it to store values,
  // so value is not stored in mass tree, use `null` as value
  mass_tree_put(mt, (const void *)hello, 5, (void *)0);
  assert(mass_tree_get(mt, (const void *)hello, 5) == (void *)hello);

  free_mass_tree(mt);
  return 0;
}
