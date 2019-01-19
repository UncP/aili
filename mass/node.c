/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#define _BSD_SOURCE

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <endian.h>
#include <stdio.h>

#include "node.h"

#ifdef Allocator
#include "../palm/allocator.h"
#endif // Allocator

#define max_key_count  15

#define magic_unstable ((uint8_t)0x10)
#define magic_link     ((uint8_t)0x20)

// `permutation` is uint64_t
#define get_count(permutation) ((int)(permutation & 0xf))
#define get_index(permutation, index) ((int)((permutation >> (((index) + 1) * 4)) & 0xf))
#define update_permutation(permutation, index, value) { \
  uint64_t mask = (((uint64_t)1) << (((index) + 1) * 4)) - 1; \
  permutation = ((permutation & (~mask)) << 4) | (((uint64_t)(value)) << (((index) + 1) * 4)) | ((permutation & mask) + 1); \
}

#define set_sequential_permutation(permutation, count) \
  (permutation = ((uint64_t)0xedcba98765432100) | (count))

// this is a little bit tricky, since both `interior_node` and `border_node`
// start with `version` field, we can convert them to `node` so that coding is easier
struct node
{
  uint32_t version;
  uint32_t removed;     // indicate empty slot
  uint64_t permutation;
  uint64_t keyslice[15];

  struct node *parent; // interior node
};

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct interior_node
{
  /* public fields */
  uint32_t version;
  uint32_t removed;
  uint64_t permutation; // this field is uint8_t in the paper,
                        // but it will generate too many intermediate states,
                        // so I changed it to uint64_t, same as in border_node
  uint64_t keyslice[15];
  node *parent;

  /* private fields */
  node    *child[16];
}interior_node;

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct border_node
{
  /* public fields */
  uint32_t version;
  uint32_t removed;
  uint64_t permutation;
  uint64_t keyslice[15];
  node *parent;

  /* private fields */

  uint8_t  padding; // for alignment
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

static int node_get_count(node *n);

static interior_node* new_interior_node()
{
#ifdef Allocator
  interior_node *in = (interior_node *)allocator_alloc(sizeof(interior_node));
#else
  interior_node *in = (interior_node *)malloc(sizeof(interior_node));
#endif // Allocator

  in->version = 0;

  in->removed = 0;

  in->permutation = 0;

  // `in->keyslice` does not need initialization

  in->parent  = 0;

  // `in->child` does not need initialization

  return in;
}

static void free_border_node(border_node *bn)
{
#ifdef Allocator
  (void)bn;
#else
  int count = node_get_count((node *)bn);

  for (int i = 0; i < count; ++i) {
    assert(bn->keylen[i] != magic_unstable);
    if (bn->keylen[i] == magic_link)
      free_node(bn->lv[i]);
    else
      free(bn->suffix[i]);
  }

  free((void *)bn);
#endif // Allocator
}

static border_node* new_border_node()
{
#ifdef Allocator
  border_node *bn = (border_node *)allocator_alloc(sizeof(border_node));
#else
  border_node *bn = (border_node *)malloc(sizeof(border_node));
#endif // Allocator

  uint32_t version = 0;
  bn->version = set_border(version);

  bn->removed = 0;

  bn->permutation = 0;

  // `bn->keyslice` does not need initialization

  bn->parent = 0;

  // set `bn->padding` and `bn->keylen[15]` to 0
  memset(&bn->padding, 0, 16);

  // `bn->suffix` and `bn->lv` does not need initialization

  bn->prev = 0;
  bn->next = 0;

  return bn;
}

static void free_interior_node(interior_node *in)
{
#ifdef Allocator
  (void)in;
#else
  int count = node_get_count((node *)in);

  for (int i = 0; i < count; ++i)
    free_node(in->child[i]);

  free((void *)in);
#endif // Allocator
}

node* new_node(int type)
{
  return likely(type == Border) ? (node *)new_border_node() : (node *)new_interior_node();
}

void free_node(node *n)
{
  uint32_t version = node_get_version(n);
  if (likely(is_border(version)))
    free_border_node((border_node *)n);
  else
    free_interior_node((interior_node *)n);
}

// fetch key and child pointer
// TODO: optimize this?
inline void node_prefetch(node *n)
{
  // 0 means for read,
  // 0 means node has a low degree of temporal locality to stay in all levels of cache if possible
  __builtin_prefetch((char *)n +   0, 0 /* rw */, 0 /* locality */);
  __builtin_prefetch((char *)n +  64, 0 /* rw */, 0 /* locality */);
  __builtin_prefetch((char *)n + 128, 0 /* rw */, 0 /* locality */);
  __builtin_prefetch((char *)n + 192, 0 /* rw */, 0 /* locality */);
}

// fetch border node `suffix` and `lv` for write
inline void border_node_prefetch_write(node *n)
{
  // 0 means for read, 1 means for write
  // 0 means node has a low degree of temporal locality to stay in all levels of cache if possible
  __builtin_prefetch((char *)n + 256, 1 /* rw */, 0 /* locality */);
  __builtin_prefetch((char *)n + 320, 1 /* rw */, 0 /* locality */);
}

// fetch border node `suffix` and `lv` for read
inline void border_node_prefetch_read(node *n)
{
  // 0 means for read, 1 means for write
  // 0 means node has a low degree of temporal locality to stay in all levels of cache if possible
  __builtin_prefetch((char *)n + 256, 0 /* rw */, 0 /* locality */);
  __builtin_prefetch((char *)n + 320, 0 /* rw */, 0 /* locality */);
}

inline uint32_t node_get_version(node *n)
{
  uint32_t version;
  __atomic_load(&n->version, &version, __ATOMIC_ACQUIRE);
  return version;
}

inline uint32_t node_get_version_unsafe(node *n)
{
  return n->version;
}

inline void node_set_version(node *n, uint32_t version)
{
  __atomic_store(&n->version, &version, __ATOMIC_RELEASE);
}

static inline void node_set_version_unsafe(node *n, uint32_t version)
{
  n->version = version;
}

static inline uint64_t node_get_permutation(node *n)
{
  uint64_t permutation;
  __atomic_load(&n->permutation, &permutation, __ATOMIC_ACQUIRE);
  return permutation;
}

static inline uint64_t node_get_permutation_unsafe(node *n)
{
  return n->permutation;
}

static inline void node_set_permutation(node *n, uint64_t permutation)
{
  __atomic_store(&n->permutation, &permutation, __ATOMIC_RELEASE);
}

static inline void node_set_permutation_unsafe(node *n, uint64_t permutation)
{
  n->permutation = permutation;
}

static inline int node_get_count(node *n)
{
  uint64_t permutation = node_get_permutation(n);
  return get_count(permutation);
}

static inline int node_get_count_unsafe(node *n)
{
  uint64_t permutation = node_get_permutation_unsafe(n);
  return get_count(permutation);
}

inline node* node_get_parent(node *n)
{
  node *parent;
  __atomic_load(&n->parent, &parent, __ATOMIC_ACQUIRE);
  return parent;
}

static inline void node_set_parent(node *n, node *p)
{
  __atomic_store(&n->parent, &p, __ATOMIC_RELEASE);
}

inline node* node_get_next(node *n)
{
  border_node *next;
  __atomic_load(&((border_node *)n)->next, &next, __ATOMIC_ACQUIRE);
  return (node *)next;
}

uint32_t node_get_stable_version(node *n)
{
  uint32_t version;
  do {
    version = node_get_version(n);
  } while (is_inserting(version) || is_spliting(version));
  return version;
}

void node_set_root_unsafe(node *n)
{
  node_set_version_unsafe(n, set_root(node_get_version_unsafe(n)));
}

void node_unset_root_unsafe(node *n)
{
  node_set_version_unsafe(n, unset_root(node_get_version_unsafe(n)));
}

void node_lock_unsafe(node *n)
{
  node_set_version_unsafe(n, set_lock(node_get_version_unsafe(n)));
}

void node_unlock_unsafe(node *n)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version));

  if (is_inserting(version)) {
    version = incr_vinsert(version);
    version = unset_insert(version);
  }
  if (is_spliting(version)) {
    version = incr_vsplit(version);
    version = unset_split(version);
  }

  node_set_version_unsafe(n, unset_lock(version));
}

void node_lock(node *n)
{
  while (1) {
    uint32_t version = n->version;
    if (is_locked(version)) {
      // __asm__ __volatile__ ("pause");
      continue;
    }
    if (__atomic_compare_exchange_n(&n->version, &version, set_lock(version),
      1 /* weak */, __ATOMIC_RELEASE, __ATOMIC_RELAXED))
      break;
  }
}

// require: `n` is locked
void node_unlock(node *n)
{
  // since `n` is locked by this thread, we can use `unsafe` operation
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version));

  if (is_inserting(version)) {
    version = incr_vinsert(version);
    version = unset_insert(version);
  }
  if (is_spliting(version)) {
    version = incr_vsplit(version);
    version = unset_split(version);
  }

  node_set_version(n, unset_lock(version));
}

node* node_get_locked_parent(node *n)
{
  node *parent;
  while (1) {
    if ((parent = node_get_parent(n)) == 0)
      break;
    node_lock(parent);
    if (node_get_parent(n) == parent)
      break;
    node_unlock(parent);
  }
  return parent;
}

// require: `n` is locked
inline int node_is_full(node *n)
{
  return node_get_count_unsafe(n) == max_key_count;
}

inline int compare_key(uint64_t k1, uint64_t k2)
{
  return (k1 > k2) - (k2 > k1);
}

inline uint64_t get_next_keyslice(const void *key, uint32_t len, uint32_t off)
{
  uint64_t cur = 0;
  assert(off <= len);
  if ((off + sizeof(uint64_t)) > len)
    memcpy(&cur, key + off, len - off); // other bytes will be 0
  else
    cur = *((uint64_t *)((char *)key + off));

  return htobe64(cur);
}

inline uint64_t get_next_keyslice_and_advance(const void *key, uint32_t len, uint32_t *off)
{
  uint64_t cur = 0;
  assert(*off <= len);
  if ((*off + sizeof(uint64_t)) > len) {
    memcpy(&cur, key + *off, len - *off); // other bytes will be 0
    *off = len;
  } else {
    cur = *((uint64_t *)((char *)key + *off));
    *off += sizeof(uint64_t);
  }

  return htobe64(cur);
}

static inline uint64_t get_next_keyslice_and_advance_and_record(const void *key, uint32_t len, uint32_t *off,
  uint8_t *keylen)
{
  uint64_t cur = 0;
  assert(*off <= len);
  if ((*off + sizeof(uint64_t)) > len) {
    memcpy(&cur, key + *off, len - *off); // other bytes will be 0
    *keylen = len - *off;
    *off = len;
  } else {
    cur = *((uint64_t *)((char *)key + *off));
    *keylen = sizeof(uint64_t);
    *off += sizeof(uint64_t);
  }

  return htobe64(cur);
}

// require: `n` is border node
int node_include_key(node *n, uint64_t cur)
{
  int index = get_index(node_get_permutation(n), 0);
  // lower key must be at index 0
  assert(index == 0);

  return compare_key(n->keyslice[0], cur) <= 0;
}

// require: `n` is locked and is interior node
inline void node_set_first_child(node *n, node *c)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_interior(version));

  c->parent = n;
  interior_node *in = (interior_node *)n;
  in->child[0] = c;
}

// require: `n` is locked and is border node
int node_get_conflict_key_index(node *n, const void *key, uint32_t len, uint32_t off, void **ckey, uint32_t *clen)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  uint64_t cur = get_next_keyslice(key, len, off);

  uint64_t permutation = node_get_permutation_unsafe(n);
  int count = get_count(permutation);

  assert(count);

  // just do a linear search
  int i = 0, index;
  for (; i < count; ++i) {
    index = get_index(permutation, i);
    if (n->keyslice[index] == cur)
      break;
  }

  // must have same key slice
  assert(i != count);

  border_node *bn = (border_node *)n;
  *ckey = bn->suffix[index];
  *clen = *(uint32_t *)&(bn->lv[index]);

  return index;
}

// replace value with new node
// require: `n` is locked and is border node
void node_replace_at_index(node *n, int index, node *n1)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  // set parent pointer
  n1->parent = n;

  border_node *bn = (border_node *)n;

  assert(bn->keylen[index] != magic_unstable);

  uint8_t unstable = magic_unstable;
  __atomic_store(&bn->keylen[index], &unstable, __ATOMIC_RELEASE);

  bn->lv[index] = n1;
  bn->suffix[index] = 0;

  uint8_t link = magic_link;
  __atomic_store(&bn->keylen[index], &link, __ATOMIC_RELEASE);
}

// require: `n` is locked and is border node
void node_swap_child(node *n, node *c, node *c1)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  uint64_t permutation = node_get_permutation_unsafe(n);
  int count = get_count(permutation);

  assert(count);

  // just do a linear search
  int i = 0, index;
  border_node *bn = (border_node *)n;
  for (; i < count; ++i) {
    index = get_index(permutation, i);
    if (bn->lv[index] == (void *)c)
      break;
  }

  // must have this child
  assert(i != count);

  node_replace_at_index(n, index, c1);
}

// require: `n` is interior node
node* node_descend(node *n, uint64_t cur)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_interior(version));

  uint64_t permutation = node_get_permutation(n);

  int first = 0, count = get_count(permutation);
  while (count > 0) {
    int half = count >> 1;
    int middle = first + half;

    int index = get_index(permutation, middle);

    int r = compare_key(n->keyslice[index], cur);
    if (r <= 0) {
      first = middle + 1;
      count -= half + 1;
    } else {
      count = half;
    }
  }

  int index = likely(first) ? (get_index(permutation, first - 1) + 1) : 0;

  return ((interior_node *)n)->child[index];
}

// require: `n` is locked and is interior node
void interior_node_insert(node *n, uint64_t key, node *child)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_interior(version));

  uint64_t permutation = node_get_permutation_unsafe(n);
  int low = 0, count = get_count(permutation), high = count - 1;
  assert(count < max_key_count);

  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    int r = compare_key(n->keyslice[index], key);
    assert(r);
    if (r < 0)
      low  = mid + 1;
    else // r > 0
      high = mid - 1;
  }

  int index;
  // now get physical slot
  if (n->removed) {
    index = ffs(n->removed) - 1;
    n->removed &= ~((uint32_t)(1 << index));
  } else {
    index = count;
  }

  node_set_version(n, set_insert(version));

  interior_node *in = (interior_node *)n;
  in->keyslice[index] = key;
  in->child[index + 1] = child;
  child->parent = n;

  update_permutation(permutation, low, index);
  node_set_permutation(n, permutation);
}

// require: `n` is locked and is border node
// if succeed, return 1;
// if existed, return 0;
// if need to go to a deeper layer, return that layer's pointer;
// if need to create a new layer, return -1
void* border_node_insert(node *n, const void *key, uint32_t len, uint32_t off, const void *val, int is_link)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  border_node *bn = (border_node *)n;

  uint8_t keylen;
  uint64_t cur = get_next_keyslice_and_advance_and_record(key, len, &off, &keylen);

  uint64_t permutation = node_get_permutation_unsafe(n);
  int low = 0, count = get_count(permutation), high = count - 1;

  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    int r = compare_key(n->keyslice[index], cur);

    if (r < 0) {
      low  = mid + 1;
    } else if (r > 0) {
      high = mid - 1;
    } else {
      uint8_t status = bn->keylen[index];
      assert(status != magic_unstable);
      if (status == magic_link) {
        // need to go to a deeper layer
        return bn->lv[index];
      } else {
        uint32_t clen = *(uint32_t *)&(bn->lv[index]);
        uint32_t coff = *((uint32_t *)&(bn->lv[index]) + 1);
        assert(coff == off);
        if (clen == len && !memcmp((char *)key + off, (char *)(bn->suffix[index]) + off, len - off))
          // key existed
          return (void *)0;
        // need to create a deeper layer
        return (void *)-1;
      }
    }
  }

  // node is full
  if (count == max_key_count)
    return (void *)-2;

  // now get physical slot
  int index;
  if (n->removed) {
    index = ffs(n->removed) - 1;
    n->removed &= ~((uint32_t)(1 << index));
    node_set_version(n, set_insert(version));
  } else {
    index = count;
  }

  bn->keyslice[index] = cur;

  if (likely(is_link == 0)) {
    bn->keylen[index] = keylen;
    bn->suffix[index] = (void *)key;
    uint32_t *len_ptr = (uint32_t *)&(bn->lv[index]);
    uint32_t *off_ptr = len_ptr + 1;
    *len_ptr = len;
    *off_ptr = off;
  } else {
    node *child = (node *)val;
    child->parent = n;
    bn->keylen[index] = magic_link;
    bn->suffix[index] = 0;
    bn->lv[index] = (void *)child;
  }

  update_permutation(permutation, low, index);
  node_set_permutation(n, permutation);

  return (void *)1;
}

// require: `n` is border node
void* node_search(node *n, uint64_t cur, void **value)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_border(version));

  *value = 0;

  uint64_t permutation = node_get_permutation(n);

  int low = 0, high = get_count(permutation) - 1;
  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    int r = compare_key(n->keyslice[index], cur);

    if (r < 0) {
      low  = mid + 1;
    } else if (r > 0) {
      high = mid - 1;
    } else {
      border_node *bn = (border_node *)n;
      uint8_t status;
      __atomic_load(&bn->keylen[index], &status, __ATOMIC_ACQUIRE);
      if (unlikely(status == magic_unstable))
        // has intermediate state, need to retry
        return (void *)1;
      if (status != magic_link)
        // NOTE: if we put key info within suffix, things will be easier
        *value = bn->suffix[index];
      return bn->lv[index];
    }
  }
  return 0;
}

// require: `bn` and `bn1` is locked
static uint64_t border_node_split(border_node *bn, border_node *bn1)
{
  uint64_t permutation = node_get_permutation_unsafe((node *)bn);

  assert(get_count(permutation) == max_key_count);

  // make sure lower key is where we want it to be
  assert(get_index(permutation, 0) == 0 || bn->prev == 0);

  assert(bn->removed == 0);

  // move half higher key to new node
  for (int i = 8, j = 0; i < max_key_count; ++i, ++j) {
    int index = get_index(permutation, i);
    bn1->keyslice[j] = bn->keyslice[index];
    bn1->keylen[j]   = bn->keylen[index];
    bn1->suffix[j]   = bn->suffix[index];
    bn1->lv[j]       = bn->lv[index];
    if (unlikely(bn1->keylen[j] == magic_link))
      node_set_parent((node *)bn1->lv[j], (node *)bn1);
    bn->removed |= (1 << index); // record empty slot
  }

  // update new node's permutation
  uint64_t npermutation;
  set_sequential_permutation(npermutation, 7);
  // it's ok to use `unsafe` opeartion,
  node_set_permutation_unsafe((node *)bn1, npermutation);
  // due to this `release` operation
  // update old node's permutation
  permutation -= 7;
  node_set_permutation((node *)bn, permutation);

  // finally modify `next` and `prev` pointer
  border_node *old_next = bn->next;
  bn1->prev = bn;
  bn1->next = old_next;
  if (old_next) old_next->prev = bn1;
  // `__ATOMIC_RELEASE` will make sure all link operations above been seen by other threads
  __atomic_store(&bn->next, &bn1, __ATOMIC_RELEASE);

  return bn1->keyslice[0];
}

// require: `in` and `in1` is locked
static uint64_t interior_node_split(interior_node *in, interior_node *in1)
{
  uint64_t permutation = node_get_permutation_unsafe((node *)in);
  assert(get_count(permutation) == max_key_count);

  assert(in->removed == 0);

  int index = get_index(permutation, 7);
  uint64_t fence = in->keyslice[index];
  in1->child[0] = in->child[index + 1];
  node_set_parent(in1->child[0], (node *)in1);
  in->removed |= (1 << index);

  // move half higher key to new node
  for (int i = 8, j = 0; i < max_key_count; ++i, ++j) {
    int index = get_index(permutation, i);
    in1->keyslice[j] = in->keyslice[index];
    in1->child[j + 1] = in->child[index + 1];
    node_set_parent(in1->child[j + 1], (node *)in1);
    in->removed |= (1 << index);
  }

  // update new node's permutation
  uint64_t npermutation;
  set_sequential_permutation(npermutation, 7);
  // it's ok to use `unsafe` opeartion,
  node_set_permutation_unsafe((node *)in1, npermutation);
  // due to this `release` operation
  // update old node's permutation
  permutation -= 8;
  node_set_permutation((node *)in, permutation);

  return fence;
}

// require: `n` is locked
node* node_split(node *n, uint64_t *fence)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version));

  int border = is_border(version);
  node *n1 = new_node(border ? Border : Interior);

  version = set_split(version);
  // it's ok to use `unsafe` operation,
  node_set_version_unsafe(n1, version);
  // due to this `release` operation
  node_set_version(n, version);

  if (border)
    *fence = border_node_split((border_node *)n, (border_node *)n1);
  else
    *fence = interior_node_split((interior_node *)n, (interior_node *)n1);

  return n1;
}

#ifdef Test

void free_node_raw(node *n)
{
  free((void *)n);
}

static void border_node_print_at_index(border_node *bn, int index)
{
  assert(bn->keylen[index] != magic_unstable);
  if (bn->keylen[index] != magic_link) {
    char buf[256];
    uint32_t len = *(uint32_t *)&(bn->lv[index]);
    uint32_t off = *((uint32_t *)&(bn->lv[index]) + 1);
    memcpy(buf, bn->suffix[index], len);
    buf[len] = 0;
    printf("slicelen: %u offset: %u  %s\n", bn->keylen[index], off, buf);
  } else {
    // node_print((node *)bn->lv[index]);
    printf("subtree\n");
  }
}

static void interior_node_print_at_index(interior_node *in, int index)
{
  node *n = in->child[index];
  uint32_t v = node_get_version(n);
  if (is_border(v)) {
    // node_print(n);
    printf("\n");
  } else {
    printf("\n");
  }
}

void node_print(node *n)
{
  uint32_t version = node_get_version(n);
  uint64_t permutation = node_get_permutation(n);
  int count = get_count(permutation);

  printf("%p\n", n);
  printf("is_root:      %u\n", !!is_root(version));
  printf("is_border:    %u\n", !!is_border(version));
  printf("is_locked:    %u\n", !!is_locked(version));
  printf("is_inserting: %u  vinsert: %u\n", !!is_inserting(version), get_vinsert(version));
  printf("is_spliting:  %u  vsplit:  %u\n", !!is_spliting(version), get_vsplit(version));
  printf("parent:  %p\n\n", node_get_parent(n));

  if (is_interior(version)) {
    printf("child count: %d\n\n", count + 1);
    interior_node_print_at_index((interior_node *)n, 0);
  }

  for (int i = 0; i < count; ++i) {
    int index = get_index(permutation, i);
    char buf[9];
    uint64_t key = htobe64(n->keyslice[index]);
    memcpy(buf, &key, sizeof(uint64_t));
    buf[8] = 0;
    printf("%-2d %-2d slice: %s  ", i, index, buf);
    if (is_border(version))
      border_node_print_at_index((border_node *)n, index);
    else
      interior_node_print_at_index((interior_node *)n, index + 1);
  }
  printf("\n");
}

static void validate(node *n)
{
  uint32_t version = node_get_version(n);
  uint64_t permutation = node_get_permutation(n);
  int count = get_count(permutation);

  if (count > 2) {
    uint64_t pre = n->keyslice[get_index(permutation, 1)];
    for (int i = 2; i < count; ++i) {
      uint64_t cur = n->keyslice[get_index(permutation, i)];
      int r = compare_key(pre, cur);
      if (r >= 0) {
        node_print(n);
      }
      assert(r < 0);
      pre = cur;
    }
  }

  uint64_t my_first = n->keyslice[get_index(permutation, 0)];
  uint64_t my_last = n->keyslice[get_index(permutation, count - 1)];
  if (is_border(version)) {
    border_node *bn = (border_node *)n;
    if (bn->prev) {
      uint64_t ppermutation = node_get_permutation((node *)bn->prev);
      uint64_t their_last = bn->prev->keyslice[get_index(ppermutation, get_count(ppermutation) - 1)];
      assert(compare_key(their_last, my_first) < 0);
    }
    if (bn->next) {
      uint64_t npermutation = node_get_permutation((node *)bn->next);
      uint64_t their_first = bn->next->keyslice[get_index(npermutation, 0)];

      int r = compare_key(my_last, their_first);
      if (r >= 0)
      {
        char buf1[9];
        memcpy(buf1, &my_last, sizeof(uint64_t));
        buf1[8] = 0;
        char buf2[9];
        memcpy(buf2, &their_first, sizeof(uint64_t));
        buf2[8] = 0;
        printf("%s %s\n", buf1, buf2);
        node_print(n);
        node_print((node *)bn->next);
      }
      assert(r < 0);
    }
  } else {
    interior_node *in = (interior_node *)n;
    uint64_t fpermutation = node_get_permutation(in->child[0]);
    uint64_t lpermutation = node_get_permutation(in->child[get_index(permutation, get_count(permutation) - 1) + 1]);
    uint64_t their_last = in->child[0]->keyslice[get_index(fpermutation, get_count(fpermutation) - 1)];
    uint64_t their_first = in->child[get_index(permutation, get_count(permutation) - 1) + 1]->keyslice[get_index(lpermutation, 0)];

    int r = compare_key(their_last, my_first);
    if (r >= 0) {
      char buf1[9];
      memcpy(buf1, &their_last, sizeof(uint64_t));
      buf1[8] = 0;
      char buf2[9];
      memcpy(buf2, &my_first, sizeof(uint64_t));
      buf2[8] = 0;
      printf("%s %s\n", buf1, buf2);
      node_print(n);
      node_print(in->child[0]);
    }
    assert(r < 0);
    r = compare_key(my_last, their_first);
    if (r > 0) {
      char buf1[9];
      memcpy(buf1, &my_last, sizeof(uint64_t));
      buf1[8] = 0;
      char buf2[9];
      memcpy(buf2, &their_first, sizeof(uint64_t));
      buf2[8] = 0;
      printf("%s %s\n", buf1, buf2);
      // node_print(n);
      node_print(in->child[count]);
    }
    assert(r <= 0); // equal is possible
  }
}

void node_validate(node *n)
{
  validate(n);
  if (is_interior(node_get_version(n))) {
    int count = node_get_count(n);
    for (int i = 0; i <= count; ++i)
      node_validate(((interior_node *)n)->child[i]);
  }
}

#endif /* Test */
