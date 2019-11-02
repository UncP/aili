/**
 *    author:     UncP
 *    date:    2019-11-02
 *    license:    BSD-3
**/

#include <stdint.h>

#include "hot_node.h"

#define node_header  \
  uint64_t type:3;   \
  uint64_t height:3; \
  uint64_t n_keys:6; \
  uint64_t locked:1; \
  uint64_t unused:51;

typedef struct hot_node_s_8
{
  node_header;
  uint8_t keys[32];
  void   *childs[32];
}hot_node_s_8;
