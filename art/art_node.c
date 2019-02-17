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

#define node_type(an) ((an)->version & node256)

typedef struct art_node4
{
  unsigned char  version;
  unsigned char  count;
  unsigned char  prefix_len;
  unsigned char  unused[1];
  unsigned char  key[4];
  char  prefix[8];
  art_node *child[4];
  char  meta[0];
}art_node4;

typedef struct art_node16
{
  unsigned char  version;
  unsigned char  count;
  unsigned char  prefix_len;
  unsigned char  unused[5];
  char  prefix[8];
  unsigned char  key[16];
  art_node *child[16];
  char  meta[0];
}art_node16;

typedef struct art_node48
{
  unsigned char version;
  unsigned char count;
  unsigned char prefix_len;
  unsigned char unused[5];
  char prefix[8];
  unsigned char index[256];
  art_node *child[48];
  char  meta[0];
}art_node48;

typedef struct art_node256
{
  unsigned char version;
  unsigned char count; // unused
  unsigned char prefix_len;
  unsigned char unused[5];
  char  prefix[8];
  art_node *child[256];
  char  meta[0];
}art_node256;

static inline art_node* _new_art_node(size_t size)
{
  art_node *an = (art_node *)malloc(size);
  an->version = 0;
  an->count = 0;
  an->prefix_len = 0;
  return an;
}

static inline art_node* new_art_node4()
{
  art_node *an = _new_art_node(sizeof(art_node4));
  an->version |= node4;
  return an;
}

static inline art_node* new_art_node16()
{
  art_node *an = _new_art_node(sizeof(art_node16));
  an->version |= node16;
  return an;
}

static inline art_node* new_art_node48()
{
  art_node *an = _new_art_node(sizeof(art_node48));
  an->version |= node48;
  memset(((art_node48 *)an)->index, 0, 256);
  return an;
}

static inline art_node* new_art_node256()
{
  art_node *an = _new_art_node(sizeof(art_node256));
  an->version |= node256;
  memset(an, 0, sizeof(art_node256));
  return an;
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
        return an4->child[i];
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
      return an16->child[__builtin_ctz(bitfield)];
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    int index = an48->index[byte];
    if (index)
      return an48->child[index - 1];
  }
  break;
  case node256:
    return ((art_node256 *)an)->child[byte];
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
  an4->child[an4->count] = child;
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
      an16->child[i] = an4->child[i];
    }
    an16->count = an4->count;
  }
  break;
  case node16: {
    art_node48 *an48 = (art_node48 *)(new = new_art_node48());
    art_node16 *an16 = (art_node16 *)*ptr;
    debug_assert(an16->count == 16);
    for (int i = 0; i < an16->count; ++i) {
      an48->child[an48->count] = an16->child[i];
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
        an256->child[i] = an48->child[index - 1];
    }
  }
  break;
  default:
    // node256 is not growable
    assert(0);
  }
  *ptr = new;
}

void* art_node_find_value(art_node *an, const void *key, size_t len, size_t off)
{
  debug_assert(is_leaf(an));

  if (len == off) return 0;

  debug_assert(len == off - 1);

  unsigned char byte = ((unsigned char *)key)[off];

  return art_node_find_child(an, byte);
}

static inline char* art_node_get_prefix(art_node *an)
{
  switch (node_type(an)) {
  case node4:
    return ((art_node4 *)an)->prefix;
  case node16:
    return ((art_node16 *)an)->prefix;
  case node48:
    return ((art_node48 *)an)->prefix;
  case node256:
    return ((art_node256 *)an)->prefix;
  default:
    assert(0);
  }
}

void art_node_set_prefix(art_node *an, const void *key, size_t off, int prefix_len)
{
  char *prefix = art_node_get_prefix(an);
  memcpy(prefix, (char *)key + off, prefix_len);
  an->prefix_len = prefix_len;
}

// return the first offset that differs
int art_node_prefix_compare(art_node *an, const void *key, size_t len, size_t off)
{
  debug_assert(off < len);

  int prefix_len = an->prefix_len;
  const char *prefix = art_node_get_prefix(an), *cur = (const char *)key;

  int i = 0;
  for (; i < prefix_len && off < len; ++i) {
    if (prefix[i] != cur[off + i])
      return i;
  }

  return i;
}

