/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#ifndef _blink_node_h_
#define _blink_node_h_

#include "latch.h"
#include "../palm/node.h"

typedef struct node palm_node;

// blink node is basically a wrapper for palm node, but with a latch and a fence key
struct blink_node {
  latch     lock[1];
  palm_node pn[1];
};

typedef struct blink_node blink_node;

blink_node* new_blink_node(uint8_t type, uint8_t level);
void free_blink_node(blink_node *bn);
void free_blink_tree_node(blink_node *bn);
void blink_node_rlock(blink_node *bn);
void blink_node_wlock(blink_node *bn);
void blink_node_unlock(blink_node *bn);
blink_node* blink_node_descend(blink_node *bn, const void *key, uint32_t len);
int blink_node_insert(blink_node *bn, const void *key, uint32_t len, const void *val);
void* blink_node_search(blink_node *bn, const void *key, uint32_t len);
void blink_node_split(blink_node *old, blink_node *new, char *pkey, uint32_t *plen);

#endif /* _blink_node_h_ */