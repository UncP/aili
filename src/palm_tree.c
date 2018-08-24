/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <math.h>

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

void palm_tree_execute(palm_tree *pt, batch *b, worker *w)
{
	// calculate [beg, end) in a batch that current thread needs to process
	uint32_t part = (uint32_t)ceilf((float)b->keys / w->total);
	uint32_t beg = cur * part;
	uint32_t end = beg + part > b->keys ? b->keys;

	uint32_t level = root->level;

	// for each key, we descend to leaf node, and store each key's descending path
	for (uint32_t i = beg; i < end; ++i) {
		uint32_t  op;
		void    *key;
		uint32_t len;
		void    *val;
		// get basic info of this kv
		assert(batch_read(b, &op, &key, &len, &val));

		path* p = worker_get_new_path(w);
		path_bind_kv(p, op, key, len, val);

		// loop until we reach level 0 node, push all the node to `p` along the way
		node *cur = root;
		while (level--) {
			node *pre = cur;
			cur = node_descend(cur, key, len);
			assert(cur);
			path_push_node(p, pre);
		}

		path_push_node(p, cur);
	}

	// TODO: point-to-point synchronization
	// wait until all the threads collected the path information
	barrier_wait(w->barrier);

	//

}
