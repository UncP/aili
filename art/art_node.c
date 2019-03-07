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

#ifdef Debug
#include <stdio.h>
#endif

#include "art_node.h"

#define node4    0
#define node16   1
#define node48   2
#define node256  3

/**
 *   node version layout(64 bits)
 *     type                 count    prefix_len              old  lock insert expand vinsert   vexpand
 *   |  2  |      14      |    8     |    8    |     12     |  1  |  1  |  1  |  1  |    8    |    8    |
 *
**/

#define OLD_BIT    ((uint64_t)1 << 19)
#define LOCK_BIT   ((uint64_t)1 << 18)
#define INSERT_BIT ((uint64_t)1 << 17)
#define EXPAND_BIT ((uint64_t)1 << 16)

#define get_prefix_len(version)      (int)(((version) >> 32) & 0xff)
#define set_prefix_len(version, len) ((version) = ((version) & (~(((uint64_t)0xff) << 32))) | (((uint64_t)(len)) << 32))
#define get_count(version)           (int)(((version) >> 40) & 0xff)
#define set_count(version, count)    ((version) = ((version) & (~(((uint64_t)0xff) << 40))) | (((uint64_t)(count)) << 40))
#define incr_count(version)          ((version) = (version) + ((uint64_t)1 << 40))
#define get_type(version)            (int)((version) & node256)
#define set_type(version, type)      ((version) |= type)

#define is_old(version)       ((version) & OLD_BIT)
#define is_locked(version)    ((version) & LOCK_BIT)
#define is_inserting(version) ((version) & INSERT_BIT)
#define is_expanding(version) ((version) & EXPAND_BIT)

#define set_old(version)    ((version) |= OLD_BIT)
#define set_lock(version)   ((version) |= LOCK_BIT)
#define set_insert(version) ((version) |= INSERT_BIT)
#define set_expand(version) ((version) |= EXPAND_BIT)

#define unset_lock(version)   ((version) &= (~LOCK_BIT))
#define unset_insert(version) ((version) &= (~INSERT_BIT))
#define unset_expand(version) ((version) &= (~EXPAND_BIT))

#define get_vinsert(version)  ((int)(((version) >> 8) & 0xff))
#define incr_vinsert(version) ((version) = ((version) & (~((uint64_t)0xff << 8))) | (((version) + (1 << 8)) & (0xff << 8))) // overflow is handled

#define get_vexpand(version)  ((int)((version) & 0xff))
#define incr_vexpand(version) ((version) = ((version) & ~((uint64_t)0xff)) | (((version) + 1) & 0xff)) // overflow is handled

struct art_node
{
  uint64_t  version;
};

typedef struct art_node4
{
  uint64_t version;
  unsigned char key[4];
  unsigned char unused[4];
  char prefix[8];
  art_node *child[4];
  art_node *parent; // we put `parent` at last since it is not updated very often
  char meta[0];
}art_node4;

typedef struct art_node16
{
  uint64_t version;
  char prefix[8];
  unsigned char key[16];
  art_node *child[16];
  art_node *parent;
  char meta[0];
}art_node16;

typedef struct art_node48
{
  uint64_t version;
  char prefix[8];
  unsigned char index[256];
  art_node *child[48];
  art_node *parent;
  char meta[0];
}art_node48;

typedef struct art_node256
{
  uint64_t version;
  char prefix[8];
  art_node *child[256];
  art_node *parent;
  char meta[0];
}art_node256;

inline uint64_t art_node_get_version(art_node *an)
{
  uint64_t version;
  __atomic_load(&an->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

inline void art_node_set_version(art_node *an, uint64_t version)
{
  __atomic_store(&an->version, &version, __ATOMIC_RELEASE);
}

inline uint64_t art_node_get_version_unsafe(art_node *an)
{
  return an->version;
}

inline void art_node_set_version_unsafe(art_node *an, uint64_t version)
{
  __atomic_store(&an->version, &version, __ATOMIC_RELAXED);
}

static inline art_node* _new_art_node(size_t size)
{
  art_node *an = (art_node *)malloc(size);
  an->version = 0;
  return an;
}

static inline art_node* new_art_node4()
{
  art_node *an = _new_art_node(sizeof(art_node4));
  set_type(an->version, node4);
  return an;
}

static inline art_node* new_art_node16()
{
  art_node *an = _new_art_node(sizeof(art_node16));
  set_type(an->version, node16);
  return an;
}

static inline art_node* new_art_node48()
{
  art_node *an = _new_art_node(sizeof(art_node48));
  set_type(an->version, node48);
  memset(((art_node48 *)an)->index, 0, 256);
  return an;
}

static inline art_node* new_art_node256()
{
  art_node *an = _new_art_node(sizeof(art_node256));
  set_type(an->version, node256);
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

art_node** art_node_find_child(art_node *an, uint64_t version, unsigned char byte)
{
  debug_assert(is_leaf(an) == 0);

  switch (get_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4*)an;
    debug_assert(get_count(version) < 5);
    for (int i = 0, count = get_count(version); i < count; ++i)
      if (an4->key[i] == byte)
        return &an4->child[i];
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)an;
    debug_assert(get_count(version) < 17);
    __m128i key = _mm_set1_epi8(byte);
    __m128i cmp = _mm_cmpeq_epi8(key, *(__m128i *)an16->key);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    if (bitfield)
      return &an16->child[__builtin_ctz(bitfield)];
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)an;
    debug_assert(get_count(version) < 49);
    int index = an48->index[byte];
    if (index)
      return &an48->child[index - 1];
  }
  break;
  case node256:
    return &((art_node256 *)an)->child[byte];
  default:
    assert(0);
  }
  return 0;
}

// add a child to art_node4, return 0 on success, otherwise return next layer
// require: node is locked
art_node** art_node_add_child(art_node **ptr, unsigned char byte, art_node *child)
{
  debug_assert(is_leaf(an) == 0);

  int full = 0;
  uint64_t version = (*ptr)->version;

  debug_assert(is_locked(version));

  art_node **next;
  if ((next = art_node_find_child(*ptr, version, byte)))
    return next;

  // grow if necessary
  if ((full = art_node_is_full(*ptr)))
    art_node_grow(ptr);

  switch (get_type(version)) {
  case node4: {
    art_node4 *an4 = (art_node4 *)*ptr;
    debug_assert(get_count(version) < 4);
    for (int i = 0, count = get_count(version); i < count; ++i)
      debug_assert(an4->key[i] != byte);
    // no need to be ordered
    int count = get_count(version);
    an4->key[count] = byte;
    an4->child[count] = child;
    an4->version = incr_count(version);
  }
  break;
  case node16: {
    art_node16 *an16 = (art_node16 *)*ptr;
    #ifdef Debug
    __m128i key = _mm_set1_epi8(byte);
    __m128i cmp = _mm_cmpeq_epi8(key, *(__m128i *)an16->key);
    int mask = (1 << get_count(version)) - 1;
    int bitfield = _mm_movemask_epi8(cmp) & mask;
    debug_assert(bitfield == 0);
    #endif
    // no need to be ordered
    int count = get_count(version);
    an16->key[count] = byte;
    an16->child[count] = child;
    an16->version = incr_count(version);
  }
  break;
  case node48: {
    art_node48 *an48 = (art_node48 *)*ptr;
    debug_assert(an48->index[byte] == 0);
    an48->version = incr_count(version);
    an48->index[byte] = get_count(version);
    an48->child[an48->index[byte] - 1] = child;
  }
  break;
  case node256: {
    art_node256 *an256 = (art_node256 *)*ptr;
    debug_assert(an256->child[byte] == 0);
    an256->child[byte] = child;
  }
  break;
  default:
    assert(0);
  }

  if (full)
    art_node_unlock(*ptr);
  return 0;
}

// require: node is locked
inline int art_node_is_full(art_node *an)
{
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  switch (get_type(version)) {
  case node4 : return get_count(version) == 4;
  case node16: return get_count(version) == 16;
  case node48: return get_count(version) == 48;
  default: return 0;
  }
}

// require: node is locked
void art_node_grow(art_node **ptr)
{
  art_node *new;
  uint64_t version = (*ptr)->version;

  debug_assert(is_locked(version));

  switch (get_type(version)) {
  case node4: {
    art_node16 *an16 = (art_node16 *)(new = new_art_node16());
    art_node4 *an4 = (art_node4 *)*ptr;
    debug_assert(get_count(version) == 4);
    memcpy(an16->prefix, an4->prefix, 8);
    set_prefix_len(an16->version, get_prefix_len(version));
    for (int i = 0; i < 4; ++i) {
      an16->key[i] = an4->key[i];
      an16->child[i] = an4->child[i];
    }
    set_count(an16->version, 4);
  }
  break;
  case node16: {
    art_node48 *an48 = (art_node48 *)(new = new_art_node48());
    art_node16 *an16 = (art_node16 *)*ptr;
    debug_assert(get_count(version) == 16);
    memcpy(an48->prefix, an16->prefix, 8);
    set_prefix_len(an48->version, get_prefix_len(version));
    for (int i = 0; i < 16; ++i) {
      an48->child[i] = an16->child[i];
      an48->index[an16->key[i]] = i + 1;
    }
    set_count(an48->version, 16);
  }
  break;
  case node48: {
    art_node256 *an256 = (art_node256 *)(new = new_art_node256());
    art_node48 *an48 = (art_node48 *)*ptr;
    debug_assert(get_count(version) == 48);
    memcpy(an256->prefix, an48->prefix, 8);
    set_prefix_len(an256->version, get_prefix_len(version));
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
  // we must lock new node first
  assert(art_node_lock(new) == 0);
  art_node *old = *ptr;
  // replace old node first
  __atomic_store(ptr, &new, __ATOMIC_RELEASE);
  // then set original node old to let other thread know
  art_node_set_version(old, set_old(version));
}

inline const char* art_node_get_prefix(art_node *an)
{
  switch (get_type(an->version)) {
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
  char *prefix = (char *)art_node_get_prefix(an);
  memcpy(prefix, (char *)key + off, prefix_len);
  set_prefix_len(an->version, prefix_len);
}

// return the first offset that differs
int art_node_prefix_compare(art_node *an, uint64_t version, const void *key, size_t len, size_t off)
{
  debug_assert(off < len);

  int prefix_len = get_prefix_len(version);
  const char *prefix = art_node_get_prefix(an), *cur = (const char *)key;

  int i = 0;
  for (; i < prefix_len && off < len; ++i, ++off) {
    if (prefix[i] != cur[off])
      return i;
  }

  return i;
}

// require: node is locked
unsigned char art_node_truncate_prefix(art_node *an, int off)
{
  uint64_t version = an->version;

  debug_assert(off < get_prefix_len(version));

  // mark expand bit before truncate prefix
  art_node_set_version(an, set_expand(version));

  int prefix_len = get_prefix_len(version);
  char *prefix = (char *)art_node_get_prefix(an);
  unsigned char ret = prefix[off];
  for (int i = 0, j = off + 1; j < prefix_len; ++i, ++j)
    prefix[i] = prefix[j];

  set_prefix_len(version, prefix_len - off - 1);
  art_node_set_version_unsafe(an, version);

  return ret;
}

inline int art_node_version_get_prefix_len(uint64_t version)
{
  return get_prefix_len(version);
}

uint64_t art_node_get_stable_expand_version(art_node *an)
{
  uint64_t version;
  do {
    version = art_node_get_version(an);
  } while (is_expanding(version));
  return version;
}

uint64_t art_node_get_stable_insert_version(art_node *an)
{
  uint64_t version;
  do {
    version = art_node_get_version(an);
  } while (is_inserting(version));
  return version;
}

inline int art_node_version_compare_expand(uint64_t version1, uint64_t version2)
{
  return is_expanding(version1) != is_expanding(version2) || get_vexpand(version1) != get_vexpand(version2);
}

inline int art_node_version_compare_insert(uint64_t version1, uint64_t version2)
{
  return is_inserting(version1) != is_inserting(version2) || get_vinsert(version1) != get_vinsert(version2);
}

// return 0 on success, 1 on failure
int art_node_lock(art_node *an)
{
  while (1) {
    // must use `acquire` operation to avoid deadlock
    uint64_t version = art_node_get_version(an);
    if (is_locked(version)) {
      // __asm__ __volatile__ ("pause");
      continue;
    }
    if (unlikely(is_old(version)))
      return 1;
    if (__atomic_compare_exchange_n(&an->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
  return 0;
}

// require: node is locked
void art_node_unlock(art_node *an)
{
  uint64_t version = an->version;

  debug_assert(is_locked(version));

  if (is_inserting(version)) {
    incr_vinsert(version);
    unset_insert(version);
  }
  if (is_expanding(version)) {
    incr_vexpand(version);
    unset_expand(version);
  }

  art_node_set_version(an, unset_lock(version));
}

int art_node_version_is_old(uint64_t version)
{
  return is_old(version);
}

