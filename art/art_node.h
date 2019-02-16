/**
 *    author:     UncP
 *    date:    2019-02-05
 *    license:    BSD-3
**/

#ifndef _art_node_h_
#define _art_node_h_

#include <assert.h>

#ifdef Debug
#define debug_assert(v) assert(v)
#else
#define debug_assert(v)
#endif

typedef struct art_node art_node;
typedef struct art_node art_node;

#define leaf 1
#define is_leaf(an) ((uintptr_t)an & leaf)

/**
 *  layout of a node
 *         pointer        empty     type    leaf
 *  |      56 bits     |    5    |   2    |  1  |
 *
**/

art_node* new_art_node();
void free_art_node(art_node *an);

#endif /* _art_node_h_ */
