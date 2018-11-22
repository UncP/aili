/**
 *    author:     UncP
 *    date:    2018-10-04
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>

#include "../palm/palm_tree.h"

/* a simple example as how to use palm tree */

int main()
{
  static const char *hello = "hello";
  static const char *world = "world";

  palm_tree *pt = new_palm_tree(2 /* thread_number */, 4 /* queue_size */);

  batch *b1 = new_batch();
  batch_add_write(b1, (const void *)hello, 5, (const void *)world);
  palm_tree_execute(pt, b1);

  batch *b2 = new_batch();
  batch_add_read(b2, (const void *)hello, 5); // index 0
  palm_tree_execute(pt, b2);

  // wait until all the batch be executed
  palm_tree_flush(pt);

  const char *value = (const char *)batch_get_value_at(b2, 0);
  assert(value == world);
  printf("%s\n", value);

  free_batch(b1);
  free_batch(b2);
  free_palm_tree(pt);
  return 0;
}
