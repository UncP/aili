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

#define max_key_count  15
#define magic_link     13 // a magic value
#define magic_unstable 14 // a magic value

// `permutation` is uint64_t
#define get_count(permutation) ((int)(((permutation) >> 60) & 0xf))
#define get_index(permutation, index) ((int)(((permutation) >> (4 * (14 - index))) & 0xf))
#define update_permutation(permutation, index, value) {                       \
  uint64_t right = (permutation << ((index + 1) * 4)) >> ((index + 2) * 4);   \
  uint64_t left = (permutation >> ((15 - index) * 4)) << ((15 - index) * 4);  \
  uint64_t middle = ((uint64_t)value & 0xf) << ((14 - index) * 4);            \
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

static int node_get_count(node *n);

static interior_node* new_interior_node()
{
  interior_node *in = (interior_node *)malloc(sizeof(interior_node));

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
  border_node *bn = (border_node *)malloc(sizeof(border_node));

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

static inline int node_get_count(node *n)
{
  uint64_t permutation = node_get_permutation(n);
  return get_count(permutation);
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

node* node_get_next(node *n)
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

// require: `n` is not root
void node_set_root(node *n)
{
  uint32_t version = node_get_version(n);
  assert(!is_root(version));
  version = set_root(version);
  node_set_version(n, version);
}

// require: `n` is root
void node_unset_root(node *n)
{
  uint32_t version = node_get_version(n);
  assert(is_root(version));
  version = unset_root(version);
  node_set_version(n, version);
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
void node_unlock(node *n)
{
  uint32_t version = node_get_version(n);
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
int node_is_full(node *n)
{
  // TODO: remove this
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  // TODO: no need to use atomic operation
  return node_get_count(n) == max_key_count;
}

// require: `n` is border node
int node_include_key(node *n, const void *key, uint32_t len, uint32_t ptr)
{
  // TODO: remove this
  uint32_t version = node_get_version(n);
  assert(is_border(version));

  uint64_t permutation = node_get_permutation(n);
  int index = get_index(permutation, 0);

  // border node's lowkey should always be at index 0
  assert(index == 0);

  uint64_t cur = 0;
  if ((ptr + sizeof(uint64_t)) > len)
    memcpy(&cur, key, len - ptr); // other bytes will be 0
  else
    cur = *((uint64_t *)((char *)key + ptr));

  return n->keyslice[index] <= cur;
}

// require: `n` is interior node
void node_set_first_child(node *n, node *c)
{
  // TODO: remove this
  uint32_t version = node_get_version(n);
  assert(is_interior(version));

  interior_node *in = (interior_node *)n;
  in->child[0] = c;
}

// require: `n` is locked and is border node
int node_get_conflict_key(node *n, const void *key, uint32_t len, uint32_t *ptr, void **ckey, uint32_t *clen)
{
  // TODO: remove this
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

  // TODO: no need to use atomic operation
  int count = node_get_count(n);
  // just do a linear search, should not hurt performance
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
  // TODO: remove this
  uint32_t version = node_get_version(n);
  assert(is_locked(version) && is_border(version));

  border_node *bn = (border_node *)n;
  uint8_t unstable = magic_unstable;
  __atomic_store(&bn->keylen[index], &unstable, __ATOMIC_RELEASE);
  // atomic operation is not needed here
  bn->lv[index] = n1;
  uint8_t link = magic_link;
  __atomic_store(&bn->keylen[index], &link, __ATOMIC_RELEASE);
}

// require: `n` is locked and is border
void node_swap_child(node *n, node *c, node *c1)
{
  // TODO: remove this
  uint32_t version = node_get_version(n);
  assert(is_locked(version) && is_border(version));

  // TODO: no need to use atomic operation
  int count = node_get_count(n);

  // just do a linear search, should not hurt performance
  int i = 0;
  border_node *bn = (border_node *)n;
  for (; i < count; ++i)
    if (bn->lv[i] == (void *)c)
      break;

  // must have the child
  assert(i != count);

  bn->lv[i] = c1;
}

// require: `n` is interior node
node* node_locate_child(node *n, const void *key, uint32_t len, uint32_t *ptr)
{
  uint32_t version = node_get_version(n);
  assert(is_interior(version));

  uint64_t permutation = node_get_permutation(n);

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

// require: `n` is locked
void* node_insert(node *n, const void *key, uint32_t len, uint32_t *ptr, const void *val, int is_link)
{
  // TODO: no need to use atomic operation
  uint32_t version = node_get_version(n);
  assert(is_locked(version));

  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation(n);

  uint8_t  keylen;
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
      // TODO: atomic operation is not necessary
      uint8_t link;
      __atomic_load(&bn->keylen[index], &link, __ATOMIC_ACQUIRE);
      assert(link != magic_unstable);
      if (link == magic_link) {
        // need to go to a deeper layer
        return bn->lv[index];
      } else {
        uint32_t clen = *(uint32_t *)&(bn->lv[index]);
        uint32_t cptr = *((uint32_t *)&(bn->lv[index]) + 1);
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
      uint32_t *len_ptr = (uint32_t *)&(bn->lv[count]);
      uint32_t *off_ptr = len_ptr + 1;
      *len_ptr =  len;
      *off_ptr = *ptr;
    } else {
      bn->keylen[count] = magic_link;
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
node* node_search(node *n, const void *key, uint32_t len, uint32_t *ptr, void **suffix)
{
  uint32_t version = node_get_version(n);
  assert(is_border(version));

  *suffix = 0;

  uint64_t permutation = node_get_permutation(n);

  uint32_t pre = *ptr;
  uint64_t cur = 0;
  if ((*ptr + sizeof(uint64_t)) > len) {
    memcpy(&cur, key, len - *ptr); // other bytes will be 0
    *ptr = len;
  } else {
    cur = *((uint64_t *)((char *)key + *ptr));
    *ptr += sizeof(uint64_t);
  }

  int low = 0, high = get_count(permutation) - 1;
  while (low <= high) {
    int mid = (low + high) / 2;

    int index = get_index(permutation, mid);

    if (n->keyslice[index] < cur) {
      low  = mid + 1;
    } else if (n->keyslice[index] > cur) {
      high = mid - 1;
    } else {
      border_node *bn = (border_node *)n;
      uint8_t status;
      __atomic_load(&bn->keylen[index], &status, __ATOMIC_ACQUIRE);
      if (status == magic_link) {
        // need to go to a deeper layer
        return bn->lv[index];
      } else if (status == magic_unstable) {
        // restore offset for retry
        *ptr = pre;
        return (void *)1;
      } else {
        uint32_t clen = *(uint32_t *)&(bn->lv[index]);
        uint32_t cptr = *((uint32_t *)&(bn->lv[index]) + 1);
        assert(cptr == *ptr);
        if (clen == len && !memcmp((char *)key + *ptr, (char *)(bn->suffix[index]) + *ptr, len - *ptr))
          // found value
          *suffix = bn->suffix[index];
        return (void *)0;
      }
    }
  }
  return 0;
}

// require: `bn` and `bn1` is locked
static uint64_t border_node_split(border_node *bn, border_node *bn1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation((node *)bn);
  // TODO: no need to use atomic operation
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
  for (int i = 0; i < 7; ++i) {
    bn->keyslice[i] = bn1->keyslice[i];
    bn->keylen[i]   = bn1->keylen[i];
    bn->suffix[i]   = bn1->suffix[i];
    bn->lv[i]       = bn1->lv[i];
  }

  // and move the other half
  for (int i = 0; i < 8; ++i) {
    bn1->keyslice[i] = bn1->keyslice[i + 7];
    bn1->keylen[i]   = bn1->keylen[i + 7];
    bn1->suffix[i]   = bn1->suffix[i + 7];
    bn1->lv[i]       = bn1->lv[i + 7];
  }

  // then we set each node's `permutation` field
  permutation = 0;
  for (int i = 0; i < 7; ++i) update_permutation(permutation, i, i);
  node_set_permutation((node *)bn, permutation);

  update_permutation(permutation, 7, 7);
  node_set_permutation((node *)bn1, permutation);

  // finally modify `next` and `prev` pointer
  border_node *old_next;
  __atomic_load(&bn->next, &old_next, __ATOMIC_RELAXED);
  __atomic_store(&bn1->prev, &bn, __ATOMIC_RELAXED);
  __atomic_store(&bn1->next, &old_next, __ATOMIC_RELAXED);
  if (old_next)
    __atomic_store(&old_next->prev, &bn1, __ATOMIC_RELAXED);
  // `__ATOMIC_RELEASE` will make sure all the relaxed operation before been seen by other threads
  __atomic_store(&bn->next, &bn1, __ATOMIC_RELEASE);

  // return fence key
  return bn1->keyslice[0];
}

// require: `in` and `in1` is locked
static uint64_t interior_node_split(interior_node *in, interior_node *in1)
{
  // TODO: no need to use atomic operation
  uint64_t permutation = node_get_permutation((node *)in);
  // TODO: no need to use atomic operation
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
    node_set_parent(in1->child[i], (node *)in1);
  }
  in1->child[7] = in1->child[14];
  node_set_parent(in1->child[7], (node *)in1);

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
  node_set_version(n1, version);

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

void node_print(node *n, int detail)
{
  uint32_t version = node_get_version(n);
  uint64_t permutation = node_get_permutation(n);
  int count = get_count(permutation);

  printf("is_locked:    %u\n", !!is_locked(version));
  printf("is_inserting: %u  vinsert: %d\n", !!is_inserting(version), get_vinsert(version));
  printf("is_spliting:  %u  vsplit:  %d\n", !!is_spliting(version), get_vsplit(version));
  printf("is_border:    %u\n", !!is_border(version));
  printf("is_root:      %u\n", !!is_root(version));

  if (detail) {
    for (int i = 0; i < count; ++i) {
      int index = get_index(permutation, i);
      printf("%2d %llu\n", index, n->keyslice[index]);
    }
  } else {
    if (count > 0) {
      int index = get_index(permutation, 0);
      printf("%llu\n", n->keyslice[index]);
    }
    if (count > 1) {
      int index = get_index(permutation, count - 1);
      printf("%llu\n", n->keyslice[index]);
    }
  }
  printf("\n");
}

#endif /* Test */
