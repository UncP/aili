/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "node.h"

// `permutation` is uint64_t
#define get_count(permutation) ((int)(((permutation) >> 60) & 0xf))
#define incr_count(permutation) ((permutation) + ((uint64_t)1 << 60)) // will not overlow
#define get_index(permutation, index) ((int)(((permutation) >> (4 * (14 - index))) & 0xf))
#define update_permutation(permutation, index, value) {  \
  uint64_t right = permutation << ((index + 1) * 4);     \
  uint64_t left = permutation >> ((15 - index) * 4);     \
  uint64_t middle = (value & 0xf) << ((14 - index) * 4); \
  permutation = left | middle | right;                   \
  permutation = incr_count(permutation);                 \
}

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct interior_node
{
  uint32_t version;

  uint64_t permutation; // this field is uint8_t in the paper,
                        // but it will generate too many intermediate states,
                        // so I changed it to uint64_t, same as in border_node
  uint64_t keyslice[15];
  struct interior_node *parent;

  void    *child[16];
}interior_node;

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct border_node
{
  uint32_t version;
  uint64_t permutation;
  uint64_t keyslice[15];

  struct interior_node *parent;

  uint8_t  nremoved;
  uint8_t  keylen[15];

  void *lv[15];

  struct border_node *prev;
  struct border_node *next;
}border_node;

// get current 8-byte key starting at `*ptr`, `*ptr` will be updated
static uint64_t get_key_at(const void *key, uint64_t len, uint32_t *ptr)
{
  uint64_t cur = 0;
  if ((*ptr + sizeof(uint64_t)) > len) {
    memcpy(&cur, key, len - *ptr); // other bytes will be 0
    *ptr = len;
  } else {
    cur = *((uint64_t *)((char *)key + *ptr));
    *ptr += sizeof(uint64_t);
  }
  return cur;
}

static interior_node* new_interior_node()
{
  interior_node *in = (interior_node *)malloc(sizeof(interior_node));

  in->version = 0;

  in->parent  = 0;

  in->permutation = 0;

  return in;
}

static border_node* new_border_node()
{
  border_node *bn = (border_node *)malloc(sizeof(border_node));

  uint32_t version = 0;

  bn->version = set_border(version);

  bn->nremoved = 0;

  bn->permutation = 0;

  bn->prev = 0;
  bn->next = 0;

  bn->parent = 0;

  return bn;
}

node* new_node(int type)
{
  return likely(type == Border) ? (node *)new_border_node() : (node *)new_interior_node();
}

void free_node(node *n)
{
  // node type does not matter
  free((void *)n);
}

static inline uint32_t node_get_version(node *n)
{
  uint32_t version;
  __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

static inline void node_set_version(node *n, uint32_t version)
{
  __atomic_store(&n->version, &version, __ATOMIC_RELEASE);
}

static inline uint64_t node_get_permutation(node *n)
{
  uint64_t permutation;
  __atomic_load(&n->permutation, &permutation, __ATOMIC_ACQUIRE);
  return permutation;
}

static inline interior_node* node_get_parent(node *n)
{
  interior_node *parent;
  __atomic_load(&n->parent, &parent, __ATOMIC_ACQUIRE);
  return parent;
}

uint32_t node_get_stable_version(node *n)
{
  uint32_t version;
  do {
    version = node_get_version(n);
  } while (is_inserting(version) || is_spliting(version));
  return version;
}

// TODO: optimize
void node_lock(node *n)
{
  uint32_t version;
  uint32_t min, max = 128;
  while (1) {
    min = 4;
    while (1) {
      version = node_get_version(n);
      if (!is_locked(version))
        break;
      for (uint32_t i = 0; i != min; ++i)
        __asm__ __volatile__("pause" ::: "memory");
      if (min < max)
        min += min;
    }
    if (__atomic_compare_exchange_n(&n->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
}

// TODO: optimize
void node_unlock(node *n)
{
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  if (is_inserting(version)) {
    version = incr_vinsert(version);
    version = unset_insert(version);
  } else if (is_spliting(version)) {
    version = incr_vsplit(version);
    version = unset_split(version);
  }

  assert(__atomic_compare_exchange_n(&n->version, &version, unset_lock(version),
    0 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

interior_node* node_get_locked_parent(node *n)
{
  interior_node *parent;
  while (1) {
    parent = node_get_parent(n);
    node_lock((node *)parent);
    if (node_get_parent(n) == parent)
      break;
    node_unlock((node *)parent);
  }
  return parent;
}

// find the child to descend to, must be an interior node
node* node_locate_child(node *n, const void *key, uint32_t len, uint32_t *ptr)
{
  // TODO: no need to use atomic operation
  const uint32_t version = node_get_version(n);
  assert(is_interior(version));

  // TODO: no need to use atomic operation
  const uint64_t permutation = node_get_permutation(n);

  const uint64_t cur = get_key_at(key, len, ptr);

  int first = 0, count = get_count(permutation);
  while (count > 0) {
    int half = count >> 1;
    int middle = first + half;

    int index = get_index(permutation, middle);

    if (n->keyslice[index] <= cur) {
      first = middle + 1;
      count -= half + 1;
    } else {
      count = half;
    }
  }

  return (node *)(((interior_node *)n)->child[first]);
}

int node_insert(node *n, const void *key, uint32_t len, uint32_t *ptr, const void *val)
{
  // TODO: no need to use atomic operation
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  node_set_version(n, set_insert(version));

  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation(n);

  const uint64_t cur = get_key_at(key, len, ptr);

  int low = 0, count = get_count(permutation), high = count - 1;

  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    if (n->keyslice[index] == cur)
      return 0;
    else if (n->keyslice[index] < cur)
      low  = mid + 1;
    else
      high = mid - 1;
  }

  // node is full
  if (count == 15) return -1;

  if (is_border(version)) {
    border_node *bn = (border_node *)n;
    bn->lv[count] = (void *)val;
  } else {
    interior_node *in = (interior_node *)n;
    in->child[count + 1] = (void *)val;
  }
  update_permutation(permutation, low, count);
  return 1;
}
