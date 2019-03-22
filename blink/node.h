/**
 *    author:     UncP
 *    date:    2018-11-20
 *    license:    BSD-3
**/

#ifndef _blink_node_h_
#define _blink_node_h_

#include "latch.h"
#include "../palm/node.h"

typedef node palm_node;

// blink node is basically a wrapper for palm node, but with a latch and a fence key
typedef struct blink_node {
  latch     lock[1];
  char      padding[64 - (sizeof(latch) % 64)]; // only for padding
  palm_node pn[1];
}blink_node;

#define blink_node_is_root(bn)   ((int)((bn)->pn->type | Root))
#define blink_node_get_level(bn) ((int)((bn)->pn->level))
#define blink_node_get_type(bn)  ((bn)->pn->type)
#define blink_node_set_type(bn, type) ((bn)->pn->type = ((type) | Blink))
#define blink_node_get_next(bn)  ((blink_node *)((bn)->pn->next))
#define blink_node_set_first(bn, fir) ((bn)->pn->first = ((palm_node *)fir))

blink_node* new_blink_node(uint8_t type, uint8_t level);
void free_blink_node(blink_node *bn);
void free_blink_tree_node(blink_node *bn);
void blink_node_rlock(blink_node *bn);
void blink_node_wlock(blink_node *bn);
void blink_node_unlock(blink_node *bn);
blink_node* blink_node_descend(blink_node *bn, const void *key, uint32_t len);
int blink_node_insert(blink_node *bn, const void *key, uint32_t len, const void *val);
void blink_node_insert_infinity_key(blink_node *bn);
void* blink_node_search(blink_node *bn, const void *key, uint32_t len);
void blink_node_split(blink_node *old, blink_node *new, char *pkey, uint32_t *plen);
int blink_node_need_move_right(blink_node *bn, const void *key, uint32_t len);

#ifdef Test

void blink_node_print(blink_node *bn, int detail);

#endif /* Test */

#endif /* _blink_node_h_ */
