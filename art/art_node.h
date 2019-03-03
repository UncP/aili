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

#define is_leaf(ptr) ((uintptr_t)ptr & 0xf)
#define make_leaf(ptr, len) ((uintptr_t)ptr | (len & 0xf)) // fow now assume key is less than 16 bytes
#define get_leaf_key(ptr) ((const char *)((uintptr_t)ptr & ((uintptr_t)~0xf)))
#define get_leaf_len(ptr) ((size_t)((uintptr_t)ptr & 0xf))

art_node* new_art_node();
void free_art_node(art_node *an);
void art_node_add_child(art_node *an, unsigned char byte, art_node *child);
art_node** art_node_find_child(art_node *an, unsigned char byte);
int art_node_is_full(art_node *an);
void art_node_grow(art_node **ptr);
void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len);
int art_node_prefix_compare(art_node *an, const void *key, size_t len, size_t off);
unsigned char art_node_truncate_prefix(art_node *an, int off);
int art_node_get_prefix_len(art_node *an);

#endif /* _art_node_h_ */
