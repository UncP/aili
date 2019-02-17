/**
 *    author:     UncP
 *    date:    2019-02-05
 *    license:    BSD-3
**/

#ifndef _art_node_h_
#define _art_node_h_

#include <stddef.h>

#ifdef Debug
#include <assert.h>
#define debug_assert(v) assert(v)
#else
#define debug_assert(v)
#endif // Debug

typedef struct art_node art_node;

struct art_node
{
  unsigned char version;
  unsigned char count;
  unsigned char prefix_len;
};

#define leaf 1
#define is_leaf(an) (an->version & leaf)
#define set_leaf(an) (an->version |= leaf)
#define prefix_len(an) ((int)an->prefix_len)

art_node* new_art_node();
void free_art_node(art_node *an);
void art_node_add_child(art_node *an, unsigned char byte, art_node *child);
art_node* art_node_find_child(art_node *an, unsigned char byte);
void* art_node_find_value(art_node *an, const void *key, size_t len, size_t off);
void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len);
int art_node_prefix_compare(art_node *an, const void *key, size_t len, size_t off);

#endif /* _art_node_h_ */
