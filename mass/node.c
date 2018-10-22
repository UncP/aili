/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "node.h"

#define max_key_count 15
#define link_value    13 // a magic value

// `permutation` is uint64_t
#define get_count(permutation) ((int)(((permutation) >> 60) & 0xf))
#define get_index(permutation, index) ((int)(((permutation) >> (4 * (14 - index))) & 0xf))
#define update_permutation(permutation, index, value) {                       \
  uint64_t right = (permutation << ((index + 1) * 4)) >> ((index + 2) * 4);   \
  uint64_t left = (permutation >> ((15 - index) * 4)) << ((15 - index) * 4);  \
  uint64_t middle = (value & 0xf) << ((14 - index) * 4);                      \
  permutation = left | middle | right;                                        \
  permutation = permutation + ((uint64_t)1 << 60);                            \
}

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct interior_node
{
  uint32_t version;

  uint64_t permutation; // this field is uint8_t in the paper,
                        // but it will generate too many intermediate states,
                        // so I changed it to uint64_t, same as in border_node
  uint64_t keyslice[15];
  node *parent;

  node    *child[16];
}interior_node;

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct border_node
{
  uint32_t version;
  uint64_t permutation;
  uint64_t keyslice[15];

  node *parent;

  uint8_t  nremoved;
  uint8_t  keylen[15];

  // TODO: memory usage optimization
  // currently `suffix` stores the whole key,
  // and if `lv` is not a link to next layer, it stores the length of the key in the first 4 bytes,
  // and the offset in the next 4 bytes
  void *suffix[15];
  void *lv[15];

  struct border_node *prev;
  struct border_node *next;
}border_node;

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

  // set `nremoved` and `keylen[15]` to 0
  memset(&bn->nremoved, 0, 16);

  bn->permutation = 0;

  bn->parent = 0;

  bn->prev = 0;
  bn->next = 0;

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

uint32_t node_get_version(node *n)
{
  uint32_t version;
  __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

void node_set_version(node *n, uint32_t version)
{
  __atomic_store(&n->version, &version, __ATOMIC_RELEASE);
}

static inline uint64_t node_get_permutation(node *n)
{
  uint64_t permutation;
  __atomic_load(&n->permutation, &permutation, __ATOMIC_ACQUIRE);
  return permutation;
}

static inline void node_set_permutation(node *n, uint64_t permutation)
{
  __atomic_store(&n->permutation, &permutation, __ATOMIC_RELEASE);
}

node* node_get_parent(node *n)
{
  node *parent;
  __atomic_load(&n->parent, &parent, __ATOMIC_ACQUIRE);
  return parent;
}

void node_set_parent(node *n, node *p)
{
  __atomic_store(&n->parent, &p, __ATOMIC_RELEASE);
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

// require: `n` is locked
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

node* node_get_locked_parent(node *n)
{
  node *parent;
  while (1) {
    if ((parent = node_get_parent(n)) == 0)
      return 0;
    node_lock(parent);
    if (node_get_parent(n) == parent)
      break;
    node_unlock(parent);
  }
  return parent;
}

// require: `n` is locked
int node_is_full(node *n)
{
  uint64_t permutation = node_get_permutation(n);
  return get_count(permutation) == max_key_count;
}

void node_insert_first_child(node *n, node *c)
{
  interior_node *in = (interior_node *)n;
  in->child[0] = c;
}

// require: `n` is an interior node
node* node_locate_child(node *n, const void *key, uint32_t len, uint32_t *ptr)
{
  // TODO: no need to use atomic operation
  const uint32_t version = node_get_version(n);
  assert(is_interior(version));

  // TODO: no need to use atomic operation
  const uint64_t permutation = node_get_permutation(n);

  uint64_t cur = 0;
  if ((*ptr + sizeof(uint64_t)) > len) {
    memcpy(&cur, key, len - *ptr); // other bytes will be 0
    *ptr = len;
  } else {
    cur = *((uint64_t *)((char *)key + *ptr));
    *ptr += sizeof(uint64_t);
  }

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

  return ((interior_node *)n)->child[first];
}

// require: `n` is locked and is border node
int node_get_conflict(node *n, const void *key, uint32_t len, uint32_t *ptr, void **ckey, uint32_t *clen)
{
  uint32_t version = node_get_version(n);
  assert(is_locked(version) && is_border(version));

  uint64_t cur = 0;
  if ((*ptr + sizeof(uint64_t)) > len) {
    memcpy(&cur, key, len - *ptr); // other bytes will be 0
    *ptr = len;
  } else {
    cur = *((uint64_t *)((char *)key + *ptr));
    *ptr  += sizeof(uint64_t);
  }

  uint64_t permutation = node_get_permutation(n);
  int count = get_count(permutation);
  // just do a linear search, does not hurt performance
  int i = 0;
  for (; i < count; ++i)
    if (n->keyslice[i] == cur)
      break;

  // must have the same key slice
  assert(i != count);

  border_node *bn = (border_node *)n;
  *ckey = bn->suffix[i];
  *clen = *(uint32_t *)bn->lv[i];

  return i;
}

// replace value with new node
// require: `n` is locked and is border node
void node_update_at(node *n, int index, node *n1)
{
  uint32_t version = node_get_version(n);
  assert(is_locked(version) && is_border(version));

  border_node *bn = (border_node *)n;
  bn->keylen[index] = link_value;
  bn->lv[index] = n1;
}

// require: `n` is locked
void* node_insert(node *n, const void *key, uint32_t len, uint32_t *ptr, const void *val, int is_link)
{
  // TODO: no need to use atomic operation
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation(n);

  uint8_t  keylen = 0;
  uint32_t pre = *ptr;
  uint64_t cur = 0;
  if ((*ptr + sizeof(uint64_t)) > len) {
    memcpy(&cur, key, len - *ptr); // other bytes will be 0
    keylen = len - *ptr;
    *ptr = len;
  } else {
    cur = *((uint64_t *)((char *)key + *ptr));
    keylen = sizeof(uint64_t);
    *ptr  += sizeof(uint64_t);
  }

  int low = 0, count = get_count(permutation), high = count - 1;

  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    if (n->keyslice[index] < cur) {
      low  = mid + 1;
    } else if (n->keyslice[index] > cur) {
      high = mid - 1;
    } else {
      assert(is_border(version));
      border_node *bn = (border_node *)n;
      if (bn->keylen[index] == link_value) {
        // need to go to a deeper layer
        return bn->lv[index];
      } else {
        uint32_t clen = *(uint32_t *)(bn->lv[index]);
        uint32_t cptr = *(((uint32_t *)(bn->lv[index])) + 1);
        assert(cptr == *ptr);
        if (clen == len && !memcmp((char *)key + *ptr, (char *)(bn->suffix[index]) + *ptr, len - *ptr))
          // key existed
          return (void *)0;
        *ptr = pre;
        // need to create a deeper layer
        return (void *)-1;
      }
    }
  }

  // node is full
  if (count == max_key_count) {
    *ptr = pre;
    return (void *)-2;
  }

  node_set_version(n, set_insert(version));

  n->keyslice[count] = cur;

  if (likely(is_border(version))) {
    border_node *bn = (border_node *)n;
    if (likely(!is_link)) {
      bn->keylen[count] = keylen;
      bn->suffix[count] = (void *)key;
      uint32_t *len_ptr = (uint32_t *)(bn->lv[count]);
      uint32_t *off_ptr = ((uint32_t *)(bn->lv[count])) + 1;
      *len_ptr =  len;
      *off_ptr = *ptr;
    } else {
      bn->keylen[count] = link_value;
      bn->lv[count] = (void *)val;
    }
  } else {
    assert(is_link == 0);
    interior_node *in = (interior_node *)n;
    in->child[count + 1] = (node *)val;
  }

  update_permutation(permutation, low, count);

  return (void *)1;
}

// require: `bn` and `bn1` is locked
uint64_t border_node_split(border_node *bn, border_node *bn1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation((node *)bn);
  int count = get_count(permutation);
  assert(count == max_key_count);
  // first we copy all the key from `bn` to `bn1` in key order
  for (int i = 0; i < count; ++i) {
    int index = get_index(permutation, i);
    bn1->keyslice[i] = bn->keyslice[index];
    bn1->keylen[i]   = bn->keylen[index];
    bn1->suffix[i]   = bn->suffix[index];
    bn1->lv[i]       = bn->lv[index];
  }

  // then we move first half of the key from `bn1` to `bn`
  memcpy(bn->keyslice, bn1->keyslice, 7 * sizeof(uint64_t));
  memcpy(bn->keylen, bn1->keylen, 7 * sizeof(uint8_t));
  memcpy(bn->suffix, bn1->suffix, 7 * sizeof(void *));
  memcpy(bn->lv, bn1->lv, 7 * sizeof(void *));

  // and move the other half
  memmove(bn1->keyslice, &bn1->keyslice[7], 8 * sizeof(uint64_t));
  memmove(bn1->keylen, &bn1->keylen[7], 8 * sizeof(uint8_t));
  memmove(bn1->suffix, &bn1->suffix[7], 8 * sizeof(void *));
  memmove(bn1->lv, &bn1->lv[7], 8 * sizeof(void *));

  // then we set each node's `permutation` field
  permutation = 0;
  for (int i = 0; i < 7; ++i) update_permutation(permutation, i, i);
  node_set_permutation((node *)bn, permutation);

  update_permutation(permutation, 7, 7);
  node_set_permutation((node *)bn1, permutation);

  // finally modify `next` and `prev` pointer
  border_node *old_next;
  __atomic_load(&bn->next, &old_next, __ATOMIC_RELAXED);
  __atomic_store(&old_next->prev, &bn1, __ATOMIC_RELAXED);
  __atomic_store(&bn1->prev, &bn, __ATOMIC_RELAXED);
  __atomic_store(&bn1->next, &old_next, __ATOMIC_RELAXED);
  // `__ATOMIC_RELEASE` will make sure all the relaxed operation before been seen by other threads
  __atomic_store(&bn->next, &bn1, __ATOMIC_RELEASE);

  // return fence key
  return bn1->keyslice[0];
}

// require: `in` and `in1` is locked
uint64_t interior_node_split(interior_node *in, interior_node *in1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation((node *)in);
  int count = get_count(permutation);
  assert(count == max_key_count);
  // first we copy all the key from `in` to `in1` in key order
  for (int i = 0; i < count; ++i) {
    int index = get_index(permutation, i);
    in1->keyslice[i] = in->keyslice[index];
    in1->child[i]    = in->child[index + 1];
  }

  // then we move first half of the key from `bn1` to `bn`
  memcpy(in->keyslice, in1->keyslice, 7 * sizeof(uint64_t));
  memcpy(&in->child[1], in1->child, 7 * sizeof(node *));
  uint64_t fence = in1->keyslice[7];

  // and move the other half
  memmove(in1->keyslice, &in1->keyslice[8], 7 * sizeof(uint64_t));
  memmove(in1->child, &in1->child[7], 8 * sizeof(node *));

  // finally we set each node's `permutation` field
  permutation = 0;
  for (int i = 0; i < 7; ++i) update_permutation(permutation, i, i);
  node_set_permutation((node *)in, permutation);
  node_set_permutation((node *)in1, permutation);

  // return fence key
  return fence;
}

// require: `n` is locked
node* node_split(node *n, uint64_t *fence)
{
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  int border = is_border(version);
  node *n1 = new_node(border ? Border : Interior);

  version = set_split(version);
  node_set_version(n, version);
  n1->version = version; // n1 is also locked

  node *parent = node_get_parent(n);
  // TODO: is there any concurrency problem?
  n1->parent = parent;

  if (border)
    *fence = border_node_split((border_node *)n, (border_node *)n1);
  else
    *fence = interior_node_split((interior_node *)n, (interior_node *)n1);

  return n1;
}
