/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdlib.h>

#include "palm_tree.h"

palm_tree* new_palm_tree()
{
	palm_tree *pt = (palm_tree *)malloc(sizeof(palm_tree));
	pt->root = new_node(Root, 0);
	return pt;
}

void free_palm_tree(palm_tree *pt)
{

}
