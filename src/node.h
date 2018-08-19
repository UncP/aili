/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#ifndef _node_h_
#define _node_h_

#include <stdint.h>

#define Root   0
#define Branch 1
#define Leaf   2

typedef struct node
{
	uint8_t      type;
	uint8_t      level;
	uint32_t     keys;
	uint8_t      pre;
	uint32_t     off;
	struct node *next;
	struct node *first;
	char         data[0];
}node;

node* new_node(uint8_t type, uint8_t level);
void free_node(node *n);
int insert(node *n, const void *key, uint32_t len, const void *val);
node* descend(node *n, const void *key, uint32_t len);
void* search(node *n, const void *key, uint32_t len);
void print_node(node *n, int detail);

#endif /* _node_h_ */