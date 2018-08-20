/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

/**
 *   B+ tree node is k-v storage unit & internal index unit
 *
 *   layout of a node in bytes:
 *       type    level   prefix             id         keys        offset    next node  first child
 *     |   1   |   1   |   1   |   1   |     4     |     4     |     4     |     8     |     8     |
 *     |        prefix data        |                          kv paris                             |
 *     |                                     kv pairs                                              |
 *     |                                     kv pairs                                              |
 *     |                         kv pairs                              |            index          |
 *
 *
 *   layout of kv pair:
 *        key len                           ptr
 *     |     1     |        key        |     8     |
 *
 *   if node is a leaf node, ptr represents the pointer to the value
 *   if node is a internal node, ptr represents the pointer to the child nodes
 *
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

#define Root   0
#define Branch 1
#define Leaf   2

#define node_min_size  (((uint32_t)1) << 12) // 4kb
#define node_max_size  (((uint32_t)1) << 20) // 1mb

typedef struct node
{
	uint32_t    type:8;
	uint32_t   level:8;
	uint32_t     pre:8;
	uint32_t        :8;
	uint32_t     id;
	uint32_t     keys;
	uint32_t     off;
	struct node *next;
	struct node *first;
	char         data[0];
}node;

void set_node_size(uint32_t size);
node* new_node(uint8_t type, uint8_t level);
void free_node(node *n);
int node_insert(node *n, const void *key, uint32_t len, const void *val);
node* node_descend(node *n, const void *key, uint32_t len);
void* node_search(node *n, const void *key, uint32_t len);

#ifdef Test

uint32_t get_node_size();
void print_node(node *n, int detail);
void node_validate(node *n);
uint32_t node_get_key_count(node *n);

#endif /* Test */

#endif /* _node_h_ */