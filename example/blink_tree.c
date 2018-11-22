/**
 *    author:     UncP
 *    date:    2018-11-22
 *    license:    BSD-3
**/

#include <stdio.h>
#include <assert.h>

#include "../blink/blink_tree.h"

/* a simple example as how to use blink tree */

int main()
{
  static const char *hello = "hello";
  static const char *world = "world";

  blink_tree *bt = new_blink_tree(2 /* thread_number */);

  // synchronous operation
  blink_tree_write(bt, (const void *)hello, 5, (const void *)world);
  void *value;
  blink_tree_read(bt, (const void *)hello, 5, &value);
  assert(value == world);
  printf("%s\n", value);

  // asynchronous operation
  blink_tree_schedule(bt, 1 /* is_write */, (const void *)world, 5, (const void *)hello);
  blink_tree_flush(bt); // wait the job to be done
  blink_tree_read(bt, (const void *)world, 5, &value);
  assert(value == hello);
  printf("%s\n", value);

  free_blink_tree(bt);
  return 0;
}
