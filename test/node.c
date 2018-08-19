/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <assert.h>

#include "../src/node.h"

void test_new_node()
{
	node *n = new_node(Leaf, 1);

	assert(n->type == Leaf);
	assert(n->level == 1);
	assert(n->keys == 0);
	assert(n->pre == 0);
	assert(n->off == 0);
	assert(n->next == 0);
	assert(n->first == 0);

	free_node(n);
}

void test_print_node()
{

}

int main()
{

	return 0;
}