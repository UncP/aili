/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

// node type
#define Interior 0
#define Border   1

/**
 *   layout of a node's version (32 bit):
 *       lock   insert  split   delete   root   border  not-used    vsplit      vinsert
 *     |   1   |  1   |   1   |   1   |   1   |   1   |    2    |     8     |     16     |
 *
**/

#define LOCK_BIT   ((uint32_t)1 << 31)
#define INSERT_BIT ((uint32_t)1 << 30)
#define SPLIT_BIT  ((uint32_t)1 << 29)
#define DELETE_BIT ((uint32_t)1 << 28)
#define ROOT_BIT   ((uint32_t)1 << 27)
#define BORDER_BIT ((uint32_t)1 << 26)

// `vsplit` is a 8 bit field
#define get_vsplit(n)  ((uint32_t)(((n) >> 16) & 0xff))
#define incr_vsplit(n) (((n) & 0xff00ffff) | (((n) + 0x10000) & 0xff0000)) // overflow is handled

// `vinsert` is a 16 bit field
#define get_vinsert(n)  ((n) & 0xffff)
#define incr_vinsert(n) (((n) & 0xffff0000) | (((n) + 1) & 0xffff)) // overflow is handled

#define set_lock(n)   ((n) | LOCK_BIT)
#define set_insert(n) ((n) | INSERT_BIT)
#define set_split(n)  ((n) | SPLIT_BIT)
#define set_delete(n) ((n) | DELETE_BIT)
#define set_root(n)   ((n) | ROOT_BIT)
#define set_border(n) ((n) | BORDER_BIT)

#define unset_lock(n)   ((n) & (~LOCK_BIT))
#define unset_insert(n) ((n) & (~INSERT_BIT))
#define unset_split(n)  ((n) & (~SPLIT_BIT))
#define unset_root(n)  ((n) & (~ROOT_BIT))

#define is_locked(n)    ((n) & LOCK_BIT)
#define is_inserting(n) ((n) & INSERT_BIT)
#define is_spliting(n)  ((n) & SPLIT_BIT)
#define is_deleted(n)   ((n) & DELETE_BIT)
#define is_root(n)      ((n) & ROOT_BIT)
#define is_border(n)    ((n) & BORDER_BIT)
#define is_interior(n)  (!is_border(n))

typedef struct node node;

node* new_node(int type);
void free_node(node *n);
void node_prefetch(node *n);
void border_node_prefetch_write(node *n);
void border_node_prefetch_read(node *n);
void node_lock_unsafe(node *n);
void node_unlock_unsafe(node *n);
void node_lock(node *n);
void node_unlock(node *n);
void node_set_root_unsafe(node *n);
void node_unset_root_unsafe(node *n);
uint32_t node_get_version(node *n);
uint32_t node_get_version_unsafe(node *n);
uint32_t node_get_stable_version(node *n);
void node_set_version(node *n, uint32_t version);
node* node_get_next(node *n);
node* node_get_parent(node *n);
node* node_get_locked_parent(node *n);
void node_set_first_child(node *n, node *c);
int node_is_full(node *n);
int node_include_key(node *n, uint64_t off);
int node_get_conflict_key_index(node *n, const void *key, uint32_t len, uint32_t off, void **ckey, uint32_t *clen);
void node_replace_at_index(node *n, int index, node *n1);
void node_swap_child(node *n, node *c, node *c1);
node* node_descend(node *n, uint64_t cur);
void* node_insert(node *n, const void *key, uint32_t len, uint32_t off, const void *val, int is_link);
node* node_split(node *n, uint64_t *fence);
void* node_search(node *n, uint64_t cur, void **value);

int compare_key(uint64_t k1, uint64_t k2);
uint64_t get_next_keyslice(const void *key, uint32_t len, uint32_t off);
uint64_t get_next_keyslice_and_advance(const void *key, uint32_t len, uint32_t *off);

#ifdef Test

void free_node_raw(node *n);
void node_print(node *n);
void node_validate(node *n);

#endif /* Test */

#endif /* _node_h_ */