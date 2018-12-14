/**
 *    author:     UncP
 *    date:    2018-10-05
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>

#include "node.h"

#ifdef Allocator
#include "../palm/allocator.h"
#endif // Allocator

#define max_key_count  15
#define magic_link     ((uint8_t)13) // a magic value
#define magic_unstable ((uint8_t)14) // a magic value

// `permutation` is uint64_t
#define get_count(permutation) ((int)((permutation) >> 60))
#define get_index(permutation, index) ((int)(((permutation) >> (4 * (14 - (index)))) & 0xf))
#define update_permutation(permutation, index, value) {                           \
  uint64_t right = (permutation << (((index) + 1) * 4)) >> (((index) + 2) * 4);   \
  uint64_t left = (permutation >> ((15 - (index)) * 4)) << ((15 - (index)) * 4);  \
  uint64_t middle = ((uint64_t)(value) & 0xf) << ((14 - (index)) * 4);            \
  permutation = left | middle | right;                                        \
  permutation = permutation + ((uint64_t)1 << 60);                            \
}

// see Mass Tree paper figure 2 for detail, node structure is reordered for easy coding
typedef struct interior_node
{
  /* public fields */
  uint32_t version;
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
  uint64_t permutation;
  uint64_t keyslice[15];
  node *parent;

  /* private fields */

  uint8_t  nremoved; // should be `uint16_t`, but for alignment reason use `uint8_t` instead
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

  in->permutation = 0;

  // `in->keyslice` does not need initialization

  in->parent  = 0;

  // `in->child` does not need initialization

  return in;
}

static void free_border_node(border_node *bn)
{
  int count = node_get_count((node *)bn);

  for (int i = 0; i < count; ++i) {
    assert(bn->keylen[i] != magic_unstable);
    if (bn->keylen[i] == magic_link)
      free_node(bn->lv[i]);
    else
      free(bn->suffix[i]);
  }

  free((void *)bn);
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

  bn->permutation = 0;

  // `bn->keyslice` does not need initialization

  bn->parent = 0;

  // set `bn->nremoved` and `bn->keylen[15]` to 0
  memset(&bn->nremoved, 0, 16);

  // `bn->suffix` and `bn->lv` does not need initialization

  bn->prev = 0;
  bn->next = 0;

  return bn;
}

static void free_interior_node(interior_node *in)
{
  int count = node_get_count((node *)in);

  for (int i = 0; i < count; ++i)
    free_node(in->child[i]);

  free((void *)in);
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

inline void node_set_parent(node *n, node *p)
{
  __atomic_store(&n->parent, &p, __ATOMIC_RELEASE);
}

inline void node_set_parent_unsafe(node *n, node *p)
{
  n->parent = p;
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
    uint32_t version = node_get_version(n);
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
  // since `n` is locked by this thread, we can use `relaxed` operation
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
  // TODO: remove this
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version));

  return node_get_count_unsafe(n) == max_key_count;
}

inline int compare_key(uint64_t k1, uint64_t k2)
{
  return memcmp(&k1, &k2, sizeof(uint64_t));
}

inline uint64_t get_next_keyslice(const void *key, uint32_t len, uint32_t off)
{
  uint64_t cur = 0;
  assert(off <= len);
  if ((off + sizeof(uint64_t)) > len)
    memcpy(&cur, key + off, len - off); // other bytes will be 0
  else
    cur = *((uint64_t *)((char *)key + off));

  return cur;
}

static inline uint64_t get_next_keyslice_and_advance(const void *key, uint32_t len, uint32_t *off)
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

  return cur;
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

  return cur;
}

// require: `n` is border node
int node_include_key(node *n, const void *key, uint32_t len, uint32_t off)
{
  // TODO: remove this
  uint32_t version = node_get_version_unsafe(n);
  assert(is_border(version));

  // TODO: remove this
  int index = get_index(node_get_permutation(n), 0);
  // lower key must be at index 0
  assert(index == 0);

  uint64_t cur = get_next_keyslice(key, len, off);

  return compare_key(n->keyslice[0], cur) <= 0;
}

// require: `n` is locked and is interior node
inline void node_set_first_child(node *n, node *c)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_interior(version));

  interior_node *in = (interior_node *)n;
  in->child[0] = c;
}

// require: `n` is locked and is border node
int node_get_conflict_key_index(node *n, const void *key, uint32_t len, uint32_t off, void **ckey, uint32_t *clen)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  uint64_t cur = get_next_keyslice(key, len, off);

  int count = node_get_count_unsafe(n);
  // just do a linear search, should not hurt performance
  int i = 0;
  for (; i < count; ++i)
    if (n->keyslice[i] == cur)
      break;

  // must have same key slice
  assert(i && i != count);

  border_node *bn = (border_node *)n;
  *ckey = bn->suffix[i];
  *clen = *(uint32_t *)&(bn->lv[i]);

  return i;
}

// replace value with new node
// require: `n` is locked and is border node
void node_replace_at_index(node *n, int index, node *n1)
{
  assert(index);

  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  border_node *bn = (border_node *)n;

  // for safety
  uint8_t status;
  __atomic_load(&bn->keylen[index], &status, __ATOMIC_RELAXED);
  assert(status != magic_unstable);

  uint8_t unstable = magic_unstable;
  __atomic_store(&bn->keylen[index], &unstable, __ATOMIC_RELEASE);

  bn->suffix[index] = 0;
  bn->lv[index] = n1;

  uint8_t link = magic_link;
  __atomic_store(&bn->keylen[index], &link, __ATOMIC_RELEASE);
}

// require: `n` is locked and is border node
void node_swap_child(node *n, node *c, node *c1)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version) && is_border(version));

  // TODO: no need to use atomic operation
  int count = node_get_count_unsafe(n);

  // just do a linear search, should not hurt performance
  int i = 0;
  border_node *bn = (border_node *)n;
  for (; i < count; ++i)
    if (bn->lv[i] == (void *)c)
      break;

  // must have this child
  assert(i && i != count);

  node_replace_at_index(n, i, c1);
}

void node_insert_lowest_key(node *n)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_border(version));

  border_node *bn = (border_node *)n;
  bn->keyslice[0] = 0;
  bn->keylen[0]   = 0;
  bn->suffix[0]   = 0;
  bn->lv[0]       = 0;

  uint64_t permutation = 0;
  update_permutation(permutation, 0, 0);
  node_set_permutation_unsafe(n, permutation);
}

// require: `n` is interior node
node* node_descend(node *n, const void *key, uint32_t len, uint32_t off)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_interior(version));

  uint64_t permutation = node_get_permutation(n);

  uint64_t cur = get_next_keyslice(key, len, off);

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

  // {
  //   char buf[256];
  //   memcpy(buf, key, len);
  //   buf[len] = 0;
  //   printf("%u %s\n", first, buf);
  //   node_print(((interior_node *)n)->child[index]);
  // }

  return ((interior_node *)n)->child[index];
}

// require: `n` is locked
// if succeed, return 1;
// if existed, return 0;
// if need to go to a deeper layer, return that layer's pointer;
// if need to create a new layer, return -1
void* node_insert(node *n, const void *key, uint32_t len, uint32_t off, const void *val, int is_link)
{
  uint32_t version = node_get_version_unsafe(n);
  assert(is_locked(version));

  uint64_t permutation = node_get_permutation_unsafe(n);

  uint8_t  keylen;
  uint64_t cur = get_next_keyslice_and_advance_and_record(key, len, &off, &keylen);

  int low = likely(is_border(version)) ? 1 : 0, count = get_count(permutation), high = count - 1;

  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    int r = compare_key(n->keyslice[index], cur);

    if (r < 0) {
      low  = mid + 1;
    } else if (r > 0) {
      high = mid - 1;
    } else {
      assert(is_border(version));
      border_node *bn = (border_node *)n;
      uint8_t status;
      __atomic_load(&bn->keylen[index], &status, __ATOMIC_RELAXED);
      assert(status != magic_unstable);
      if (status == magic_link) {
        // need to go to a deeper layer
        return bn->lv[index];
      } else {
        uint32_t clen = *(uint32_t *)&(bn->lv[index]);
        uint32_t coff = *((uint32_t *)&(bn->lv[index]) + 1);
        assert(coff == off);
        if (clen == len && len > off &&
            !memcmp((char *)key + off, (char *)(bn->suffix[index]) + off, len - off))
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

  // set `insert` before actually write this node
  node_set_version(n, set_insert(version));

  n->keyslice[count] = cur;

  if (likely(is_border(version))) {
    border_node *bn = (border_node *)n;
    if (likely(!is_link)) {
      bn->keylen[count] = keylen;
      bn->suffix[count] = (void *)key;
      uint32_t *len_ptr = (uint32_t *)&(bn->lv[count]);
      uint32_t *off_ptr = len_ptr + 1;
      *len_ptr = len;
      *off_ptr = off;
    } else {
      bn->keylen[count] = magic_link;
      bn->suffix[count] = 0;
      bn->lv[count] = (void *)val;
    }
  } else {
    assert(is_link == 1);
    interior_node *in = (interior_node *)n;
    in->child[count + 1] = (node *)val;
  }

  update_permutation(permutation, low, count);
  node_set_permutation(n, permutation);

  return (void *)1;
}

// require: `n` is border node
node* node_search(node *n, const void *key, uint32_t len, uint32_t off, void **value)
{
  // it's ok to use `relaxed` operation here because we only use it to verify node type,
  // and node type stays the same the entire time
  uint32_t version = node_get_version_unsafe(n);
  assert(is_border(version));

  *value = 0;

  uint64_t permutation = node_get_permutation(n);

  uint64_t cur = get_next_keyslice_and_advance(key, len, &off);

  int low = 1, high = get_count(permutation) - 1;
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
      uint8_t status1, status2;
      __atomic_load(&bn->keylen[index], &status1, __ATOMIC_ACQUIRE);
      uint64_t lv = (uint64_t)bn->lv[index];
      void *suffix = bn->suffix[index];
      __atomic_load(&bn->keylen[index], &status2, __ATOMIC_ACQUIRE);
      if (status1 == magic_unstable || status2 == magic_unstable)
        // has intermediate state, need to retry
        return (void *)1;

      // check the latest status

      // could be two different links, but we will verify whether it is root when we descend
      if (status2 == magic_link)
        return (void *)lv; // need to go to a deeper layer
      // could be two different values, but that means an split happened, which will be invalidated
      // when we return
      uint32_t clen = (uint32_t)lv;
      uint32_t coff = (uint32_t)(lv >> 32);
      assert(coff == off);
      if (clen == len && !memcmp((char *)key + off, (char *)suffix + off, len - off))
        *value = suffix; // value found
      return (void *)0;
    }
  }
  return 0;
}

// require: `bn` and `bn1` is locked
static uint64_t border_node_split(border_node *bn, border_node *bn1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation_unsafe((node *)bn);
  int count = get_count(permutation);
  assert(count == max_key_count);

  // make sure lower key is where we want it to be
  assert(get_index(permutation, 0) == 0);

  // first we copy all the key[1, 14](except lower key) from `bn` to `bn1` in key order
  for (int i = 0; i < count - 1; ++i) {
    int index = get_index(permutation, i + 1);
    bn1->keyslice[i] = bn->keyslice[index];
    bn1->keylen[i]   = bn->keylen[index];
    bn1->suffix[i]   = bn->suffix[index];
    bn1->lv[i]       = bn->lv[index];
  }

  // then we move first half [1-7] of the key from `bn1` to `bn`
  for (int i = 0; i < 7; ++i) {
    bn->keyslice[i+1] = bn1->keyslice[i];
    bn->keylen[i+1]   = bn1->keylen[i];
    bn->suffix[i+1]   = bn1->suffix[i];
    bn->lv[i+1]       = bn1->lv[i];
  }

  // and move the other half [8, 14]
  for (int i = 0; i < 7; ++i) {
    bn1->keyslice[i+1] = bn1->keyslice[i + 7];
    bn1->keylen[i+1]   = bn1->keylen[i + 7];
    bn1->suffix[i+1]   = bn1->suffix[i + 7];
    bn1->lv[i+1]       = bn1->lv[i + 7];
  }

  // set new node's lower key
  bn1->keyslice[0] = bn1->keyslice[1];
  bn1->keylen[0]   = sizeof(uint64_t);
  bn1->suffix[0]   = 0;
  bn1->lv[0]       = 0;

  // then we set each node's `permutation` field
  permutation = 0;
  for (int i = 0; i < 8; ++i) update_permutation(permutation, i, i);
  // it's ok to use `relaxed` operation,
  node_set_permutation_unsafe((node *)bn1, permutation);
  // due to this `release` operation
  node_set_permutation((node *)bn, permutation);

  // finally modify `next` and `prev` pointer
  border_node *old_next;
  __atomic_load(&bn->next, &old_next, __ATOMIC_RELAXED);
  __atomic_store(&bn1->prev, &bn, __ATOMIC_RELAXED);
  __atomic_store(&bn1->next, &old_next, __ATOMIC_RELAXED);
  // `__ATOMIC_RELEASE` will make sure all the relaxed operation above been seen by other threads
  if (old_next)
    __atomic_store(&old_next->prev, &bn1, __ATOMIC_RELEASE);
  __atomic_store(&bn->next, &bn1, __ATOMIC_RELEASE);

  // node_print((node *)bn);
  // node_print((node *)bn1);
  // `bn1->keyslice[0]` is lower key
  return bn1->keyslice[1];
}

// require: `in` and `in1` is locked
static uint64_t interior_node_split(interior_node *in, interior_node *in1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation_unsafe((node *)in);
  int count = get_count(permutation);
  assert(count == max_key_count);

  // first we copy all the key from `in` to `in1` in key order
  for (int i = 0; i < count; ++i) {
    int index = get_index(permutation, i);
    in1->keyslice[i] = in->keyslice[index];
    in1->child[i]    = in->child[index + 1];
  }

  // then we move first half of the key from `bn1` to `bn`
  for (int i = 0; i < 7; ++i) {
    in->keyslice[i] = in1->keyslice[i];
    in->child[i+1]  = in1->child[i];
  }

  uint64_t fence = in1->keyslice[7];

  // and move the other half
  for (int i = 0; i < 7; ++i) {
    in1->keyslice[i] = in1->keyslice[i + 8];
    in1->child[i]    = in1->child[i + 7];
    // must use `release` operation
    node_set_parent(in1->child[i], (node *)in1);
  }
  in1->child[7] = in1->child[14];
  // must use `release` operation
  node_set_parent(in1->child[7], (node *)in1);

  // finally we set each node's `permutation` field
  permutation = 0;
  for (int i = 0; i < 7; ++i) update_permutation(permutation, i, i);
  // it's ok to use `relaxed` opeartion here,
  node_set_permutation_unsafe((node *)in1, permutation);
  // due to this `release` operation
  node_set_permutation((node *)in, permutation);

  // node_print((node *)in);
  // node_print((node *)in1);
  // printf("\ninterior node split over !!!\n");
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
  // it's ok to use `relaxed` operation here
  node_set_version_unsafe(n1, version);
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
    memcpy(buf, &n->keyslice[index], sizeof(uint64_t));
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

  uint64_t my_first;
  uint64_t my_last = n->keyslice[get_index(permutation, count - 1)];
  if (is_border(version)) {
    my_first = n->keyslice[get_index(permutation, 1)];
    border_node *bn = (border_node *)n;
    if (bn->prev) {
      uint64_t ppermutation = node_get_permutation((node *)bn->prev);
      uint64_t their_last = bn->prev->keyslice[get_index(ppermutation, get_count(ppermutation) - 1)];
      assert(compare_key(their_last, my_first) < 0);
    }
    if (bn->next) {
      uint64_t npermutation = node_get_permutation((node *)bn->next);
      uint64_t their_first = bn->next->keyslice[get_index(npermutation, 1)];

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
    my_first = n->keyslice[get_index(permutation, 0)];

    interior_node *in = (interior_node *)n;
    uint64_t fpermutation = node_get_permutation(in->child[0]);
    uint64_t lpermutation = node_get_permutation(in->child[get_index(permutation, get_count(permutation) - 1) + 1]);
    uint64_t their_last = in->child[0]->keyslice[get_index(fpermutation, get_count(fpermutation) - 1)];
    node *last_child = in->child[get_index(permutation, get_count(permutation) - 1) + 1];
    uint64_t their_first;
    if (is_border(node_get_version(last_child)))
      their_first = last_child->keyslice[get_index(lpermutation, 1)];
    else
      their_first = last_child->keyslice[get_index(lpermutation, 0)];

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
