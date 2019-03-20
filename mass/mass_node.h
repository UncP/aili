/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#ifndef _mass_node_h_
#define _mass_node_h_

#include <stdint.h>

#define likely(x)   (__builtin_expect(!!(x), 1))
#define unlikely(x) (__builtin_expect(!!(x), 0))

// mass_node type
#define Interior 0
#define Border   1

/**
 *   layout of a mass_node's version (32 bit):
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

typedef struct mass_node mass_node;

mass_node* new_mass_node(int type);
void free_mass_node(mass_node *n);
void mass_node_prefetch(mass_node *n);
void border_mass_node_prefetch_write(mass_node *n);
void border_mass_node_prefetch_read(mass_node *n);
void mass_node_lock_unsafe(mass_node *n);
void mass_node_unlock_unsafe(mass_node *n);
void mass_node_lock(mass_node *n);
void mass_node_unlock(mass_node *n);
void mass_node_set_root_unsafe(mass_node *n);
void mass_node_unset_root_unsafe(mass_node *n);
uint32_t mass_node_get_version(mass_node *n);
uint32_t mass_node_get_version_unsafe(mass_node *n);
uint32_t mass_node_get_stable_version(mass_node *n);
void mass_node_set_version(mass_node *n, uint32_t version);
mass_node* mass_node_get_next(mass_node *n);
mass_node* mass_node_get_parent(mass_node *n);
mass_node* mass_node_get_locked_parent(mass_node *n);
void mass_node_set_first_child(mass_node *n, mass_node *c);
int mass_node_is_full(mass_node *n);
int mass_node_include_key(mass_node *n, uint64_t off);
int mass_node_get_conflict_key_index(mass_node *n, const void *key, uint32_t len, uint32_t off, void **ckey, uint32_t *clen);
void mass_node_replace_at_index(mass_node *n, int index, mass_node *n1);
void mass_node_swap_child(mass_node *n, mass_node *c, mass_node *c1);
mass_node* mass_node_descend(mass_node *n, uint64_t cur);
void* border_mass_node_insert(mass_node *n, const void *key, uint32_t len, uint32_t off, const void *val, int is_link);
void interior_mass_node_insert(mass_node *n, uint64_t key, mass_node *child);
mass_node* mass_node_split(mass_node *n, uint64_t *fence);
void* mass_node_search(mass_node *n, uint64_t cur, void **value);

int mass_compare_key(uint64_t k1, uint64_t k2);
uint64_t get_next_keyslice(const void *key, uint32_t len, uint32_t off);
uint64_t get_next_keyslice_and_advance(const void *key, uint32_t len, uint32_t *off);

#ifndef htobe64
#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define htobe64(x)  OSSwapHostToBigInt64(x)
#endif
#ifdef __linux__
#define _BSD_SOURCE
#include <endian.h>
#endif
#endif

#ifdef Test

void free_mass_node_raw(mass_node *n);
void mass_node_print(mass_node *n);
void mass_node_validate(mass_node *n);

#endif /* Test */

#endif /* _mass_mass_node_h_ */
