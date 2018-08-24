/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

/**
 *   B+ tree node is k-v storage unit & internal index unit (default)
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
 *   if node is a leaf node, ptr represents the pointer to the value or value itself
 *   if node is a internal node, ptr represents the pointer to the child nodes
 *
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

// node type
#define Root   0
#define Branch 1
#define Leaf   2

// op type
#define Read  0
#define Write 1

// do not fucking change it
typedef uint64_t val_t;
#define value_bytes sizeof(val_t)

// you can change uint8_t to uint16_t so that bigger keys are supported,
// but key length byte will take more space
typedef uint8_t  len_t;
#define key_byte sizeof(len_t)

// you can change uint16_t to uint32_t so that bigger nodes are supported,
// but index will take more space
typedef uint16_t index_t;
#define index_byte sizeof(index_t)

#define node_min_size  (((uint32_t)1) << 12) //  4kb
#define node_max_size  (((uint32_t)1) << 16) // 64kb,
                                             // if you set `index_t` to uint32_t,
                                             // the node_max_size can be up to 4gb

typedef struct node
{
	uint32_t    type:8;   // Root or Branch or Leaf
	uint32_t   level:8;
	uint32_t     pre:16;  // prefix length
	uint32_t     id;
	uint32_t     keys;    // number of keys
	uint32_t     off;     // current data offset
	struct node *next;    // pointer to the right child
	struct node *first;   // pointer to the first child if it's level > 0
	char         data[0];
}node;

// typedef struct pair
// {
// 	char      key[max_key_size + 1]; // +1 for alignment
// 	uint32_t  len;
// 	void     *val;
// }pair;

void set_node_size(uint32_t size);
node* new_node(uint8_t type, uint8_t level);
void free_node(node *n);
node* node_descend(node *n, const void *key, uint32_t len);
int node_insert(node *n, const void *key, uint32_t len, const void *val);
void* node_search(node *n, const void *key, uint32_t len);
void node_split(node *old, node *new, char *pkey, uint32_t *plen);

/**
 *   batch is a wrapper for node with some differences,
 *   key may be duplicated, also it does not support prefix compression
 *
 *   layout of kv pair in batch:
 *            op       key len                           ptr
 *      |     1     |     1     |        key        |     8     |
**/

typedef node batch;

batch* new_batch();
void free_batch(batch *b);
void batch_clear(batch *b);
int batch_write(batch *b, uint8_t op, const void *key1, uint32_t len1, const void *val);
int batch_read(batch *b, uint32_t idx, uint8_t *op, void **key, uint32_t *len, void *val);

#ifdef Test

uint32_t get_node_size();
void print_node(node *n, int detail);
void print_batch(batch *b, int detail);
void node_validate(node *n);
void batch_validate(batch *n);

#endif /* Test */

#endif /* _node_h_ */