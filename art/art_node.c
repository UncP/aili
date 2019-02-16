/**
 *    author:     UncP
 *    date:    2019-02-06
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>
#include <emmintrin.h>

#include "art_node.h"

#define node4    (0 << 1)
#define node16   (1 << 1)
#define node48   (2 << 1)
#define node256  (3 << 1)

#define node_type(an) ((uintptr_t)an & node256)

typedef struct art_node4
{
  unsigned char  key[4];
  unsigned char  count;
  unsigned char  prefix_len;
  unsigned char  unused[2];
  char  prefix[8];
  union {
    art_node *child[4];
    void     *value[4];
  }lv;
  char  meta[0];
}art_node4;

typedef struct art_node16
{
  unsigned char  key[16];
  unsigned char  count;
  unsigned char  prefix_len;
  unsigned char  unused[6];
  char  prefix[8];
  union {
    art_node *child[16];
    void     *value[16];
  }lv;
  char  meta[0];
}art_node16;

typedef struct art_node48
{
  unsigned char index[256];
  unsigned char count;
  unsigned char prefix_len;
  unsigned char unused[6];
  char prefix[8];
  union {
    art_node *child[48];
    void     *value[48];
  }lv;
  char  meta[0];
}art_node48;

typedef struct art_node256
{
  unsigned char prefix_len;
  unsigned char unused[7];
  char  prefix[8];
  union {
    art_node *child[256];
    void     *value[256];
  }lv;
  char  meta[0];
}art_node256;

static inline art_node* new_art_node4()
{
  art_node *an = (art_node *)malloc(sizeof(art_node4));
  ((art_node4 *)an)->count = 0;
  ((art_node4 *)an)->prefix_len = 0;
  return (art_node *)((uintptr_t)an | node4);
}

static inline art_node* new_art_node16()
{
  art_node *an = (art_node *)malloc(sizeof(art_node16));
  ((art_node16 *)an)->count = 0;
  ((art_node16 *)an)->prefix_len = 0;
  return (art_node *)((uintptr_t)an | node16);
}

static inline art_node* new_art_node48()
{
  art_node *an = (art_node *)malloc(sizeof(art_node48));
  memset(an, 0, 256 + 1 + 1); // `index` + `count` + `prefix_len`
  return (art_node *)((uintptr_t)an | node48);
}

static inline art_node* new_art_node256()
{
  art_node *an = (art_node *)calloc(1, sizeof(art_node256));
  return (art_node *)((uintptr_t)an | node256);
}

art_node* new_art_node()
{
  return new_art_node4();
}

void free_art_node(art_node *an)
{
  free((void *)an);
}

art_node* art_node_find_child(art_node *an, unsigned char byte)
{
  debug_assert(is_leaf(an) == 0);

  switch (node_type(an)) {
  case node4: {
    art_node4 *an4 = (art_node4*)an;
    debug_assert(an4->count < 5);
    for (int i = 0; i < an4->count; ++i)
      if (an4->key[i] == byte)
        return an4->lv.child[i];
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    debug_assert(an16->count < 17);
    __m128i key = _mm_set1_epi8(byte);
    __m128i cmp = _mm_cmpeq_epi8(key, *(__m128i *)an16->key);
    int mask = (1 << an16->count) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    if (bitfield)
      return an16->lv.child[__builtin_ctz(bitfield)];
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    int index = an48->index[byte];
    if (index)
      return an48->lv.child[index - 1];
  }
  break;
  case node256:
    return ((art_node256 *)an)->lv.child[byte];
  default:
    assert(0);
  }
  return 0;
}

// add a child to art_node4, `byte` must not exist in this node before
void art_node_add_child(art_node *an, unsigned char byte, art_node *child)
{
  debug_assert(node_type(an) == node4);

  art_node4 *an4 = (art_node4 *)an;

  debug_assert(an4->count < 4);

  #ifdef Debug
  for (int i = 0; i < an4->count; ++i)
    assert(an4->key[i] != byte);
  #endif // Debug

  // no need to be ordered
  an4->key[an4->count] = byte;
  an4->lv.child[an4->count] = child;
  ++an4->count;
}

void art_node_grow(art_node **ptr)
{
  art_node *new;

  switch(node_type(*ptr)) {
  case node4: {
    art_node16 *an16 = (art_node16 *)(new = new_art_node16());
    art_node4 *an4 = (art_node4 *)*ptr;
    debug_assert(an4->count == 4);
    for (int i = 0; i < an4->count; ++i) {
      an16->key[i] = an4->key[i];
      an16->lv.child[i] = an4->lv.child[i];
    }
    an16->count = an4->count;
  }
  break;
  case node16: {
    art_node48 *an48 = (art_node48 *)(new = new_art_node48());
    art_node16 *an16 = (art_node16 *)*ptr;
    debug_assert(an16->count == 16);
    for (int i = 0; i < an16->count; ++i) {
      an48->lv.child[an48->count] = an16->lv.child[i];
      an48->index[an16->key[i]] = ++an48->count;
    }
  }
  break;
  case node48: {
    art_node256 *an256 = (art_node256 *)(new = new_art_node256());
    art_node48 *an48 = (art_node48 *)*ptr;
    debug_assert(an48->count == 48);
    for (int i = 0; i < 256; ++i) {
      int index = an48->index[i];
      if (index)
        an256->lv.child[i] = an48->lv.child[index - 1];
    }
  }
  break;
  default:
    // node256 is not growable
    assert(0);
  }
  *ptr = new;
}

