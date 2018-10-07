/**
 *    author:     UncP
 *    date:    2018-10-07
 *    license:    BSD-3
**/

#include <assert.h>

#include "../mass/node.h"

void test_node()
{
  node* n1 = new_node(Border);
  assert(node_is_border(n1));

  assert(!node_is_locked(n1));
  n1->version = set_lock(n1->version);
  assert(node_is_locked(n1));
  n1->version = unset_lock(n1->version);
  assert(!node_is_locked(n1));

  assert(!node_is_inserting(n1));
  n1->version = set_insert(n1->version);
  assert(node_is_inserting(n1));
  n1->version = unset_insert(n1->version);
  assert(!node_is_inserting(n1));

  assert(!node_is_spliting(n1));
  n1->version = set_split(n1->version);
  assert(node_is_spliting(n1));
  n1->version = unset_split(n1->version);
  assert(!node_is_spliting(n1));

  assert(!node_is_root(n1));
  n1->version = set_root(n1->version);
  assert(node_is_root(n1));

  assert(!node_is_deleted(n1));
  n1->version = set_delete(n1->version);
  assert(node_is_deleted(n1));

  free_node(n1);

  node *n2 = new_node(Interior);
  assert(node_is_interior(n2));
  free_node(n2);
}

int main()
{
  test_node();
  return 0;
}
