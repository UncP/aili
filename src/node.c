/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "node.h"

static uint32_t node_size = node_min_size;
static uint32_t node_id = 0;

void set_node_size(uint32_t size)
{
  const uint64_t node_size_mask = (uint64_t)~0xfff;
  node_size = size < node_min_size ? node_min_size : size > node_max_size ? node_max_size : size;
  node_size &= node_size_mask;
}

// get the ptr to the key length byte
#define get_ptr(n, off) ((char *)n->data + off)
// get the length of the key
#define get_len(n, off) ((uint32_t)(*(len_t *)get_ptr(n, off)))
#define get_key(n, off) (get_ptr(n, off) + key_byte)
#define get_val(n, off) ((void *)(*(val_t *)(get_key(n, off) + get_len(n, off))))
#define node_index(n)   ((index_t *)((char *)n + (node_size - (n->keys * index_byte))))
#define get_key_info(n, off, key, len) \
  const void *key = get_key(n, off);   \
  uint32_t len = get_len(n, off);
#define get_kv_info(n, off, key, len, val) \
  const void *key = get_key(n, off);       \
  uint32_t len = get_len(n, off);          \
  void *val = get_val(n, off);
#define get_kv_ptr(n, off, key, len, val)                 \
  const void *key = get_key(n, off);                      \
  uint32_t len = get_len(n, off);                         \
  void *val = (void *)((char *)key + len);

int compare_key(const void *key1, uint32_t len1, const void *key2, uint32_t len2)
{
  uint32_t min = len1 < len2 ? len1 : len2;
  int r = memcmp(key1, key2, min);
  return r ? r : (len1 == len2 ? 0 : (len1 < len2 ? -1 : +1));
}

/****** NODE operation ******/

node* new_node(uint8_t type, uint8_t level)
{
  node* n  = (node *)malloc(node_size);
  n->type  = type;
  n->level = level;
  n->pre   = 0;
  n->id    = node_id++;
  n->keys  = 0;
  n->off   = 0;
  n->next  = 0;
  n->first = 0;

  return n;
}

void free_node(node *n)
{
  free((void *)n);
}

node* node_descend(node *n, const void *key, uint32_t len)
{
  assert(n->level && n->keys);

  index_t *index = node_index(n);
  int low = 0, high = (int)n->keys - 1;

  if (n->pre) { // compare with node prefix
    uint32_t tlen = len < n->pre ? len : n->pre;
    int r = compare_key(key, tlen, n->data, n->pre);
    if (r < 0)
      return n->first;
    if (r > 0)
      return (node *)get_val(n, index[high]);
  }

  const void *key1 = key + n->pre;
  uint32_t    len1 = len - n->pre;

  while (low < high) {
    int mid = (low + high + 1) / 2;

    get_key_info(n, index[mid], key2, len2);

    if (compare_key(key2, len2, key1, len1) < 0)
      low  = mid;
    else
      high = mid - 1;
  }

  get_kv_info(n, index[low], key2, len2, val);
  if (compare_key(key2, len2, key1, len1) < 0)
    return (node *)val;
  else
    return n->first;
}

// find the key in the leaf, return its pointer, if no such key, return null
void* node_search(node *n, const void *key, uint32_t len)
{
  assert(n->level == 0);

  if (n->keys == 0) return 0;

  if (n->pre && compare_key(key, len, n->data, n->pre)) // compare with node prefix
    return 0;

  int low = 0, high = (int)n->keys - 1;
  index_t *index = node_index(n);
  const void *key1 = key + n->pre;
  uint32_t    len1 = len - n->pre;

  while (low <= high) {
    int mid = (low + high) / 2;

    get_key_info(n, index[mid], key2, len2);

    int r = compare_key(key2, len2, key1, len1);
    if (r == 0) {
      return get_val(n, index[mid]);
    } else if (r < 0) {
      low  = mid + 1;
    } else {
      high = mid - 1;
    }
  }

  return 0;
}

static void node_insert_kv(node *n, const void *key, uint32_t len, const void *val)
{
  *((len_t *)(n->data + n->off)) = (len_t)len;
  n->off += key_byte;
  memcpy(n->data + n->off, key, len);
  n->off += len;
  if (val)
    *((val_t *)(n->data + n->off)) = *(val_t *)(&val);
  else
    *((val_t *)(n->data + n->off)) = 0;
  n->off += value_bytes;

  ++n->keys;
}

// insert a kv into node, if key already exists, return 0
// if there is not enough space, return -1
// if succeed, return 1
int node_insert(node *n, const void *key, uint32_t len, const void *val)
{
  int pos = -1;

  if (n->pre) { // compare with node prefix
    int r = compare_key(key, len, n->data, n->pre);
    if (r < 0)
      pos = 0;
    else if (r > 0)
      pos = n->keys;
  }

  // find the index which to insert the key
  if (pos == -1) {
    int low = 0, high = (int)n->keys - 1;
    index_t *index = node_index(n);
    const void *key1 = key + n->pre;
    uint32_t    len1 = len - n->pre;

    while (low <= high) {
      int mid = (low + high) / 2;

      get_key_info(n, index[mid], key2, len2);

      int r = compare_key(key2, len2, key1, len1);
      if (r == 0)
        return 0;
      else if (r < 0)
        low  = mid + 1;
      else
        high = mid - 1;
    }
    pos = low;
  }

  // key does not exist, we can proceed

  uint32_t klen = len - n->pre;
  index_t *index = node_index(n) - 1;

  // check if there is enough space
  if ((char *)n->data + (n->off + key_byte + klen + value_bytes) > (char *)index)
    return -1;

  // update index
  if (pos) memmove(&index[0], &index[1], pos * index_byte);
  index[pos] = n->off;

  node_insert_kv(n, key + n->pre, klen, val);

  return 1;
}

// split half of the node entries from `old` to `new`
void node_split(node *old, node *new, char *pkey, uint32_t *plen)
{
  uint32_t left = old->keys / 2, right = old->keys - left;
  index_t *l_idx = node_index(old), *r_idx = node_index(new);
  *plen = 0;

  if (old->pre) { // copy prefix
    memcpy(new->data, old->data, old->pre);
    new->pre = old->pre;
    new->off = new->pre;
    memcpy(pkey, old->data, old->pre);
    *plen = old->pre;
  }

  // copy promote key data
  get_kv_info(old, l_idx[left], fkey, flen, fval);
  memcpy(pkey + *plen, fkey, flen);
  *plen += flen;

  if (old->level) { // assign first child if it's not a level 0 node
    new->first = fval;
    --right;        // one key will be promoted to upper level
    flen += key_byte + value_bytes;
  } else {
    flen = 0;
  }

  // we first copy all the keys to `new` in sequential order,
  // then move the first half back to `old` and adjust the other half in `new`
  // the loop has some optimization, it does not have good readability
  uint32_t length = 0;
  r_idx -= old->keys;
  for (uint32_t i = 0, j = old->keys - right; i < old->keys; ++i) {
    r_idx[i] = new->off;
    get_kv_info(old, l_idx[i], okey, olen, oval);
    node_insert_kv(new, okey, olen, oval);
    if (i == left - 1)
      length = new->off - new->pre;
    if (i >= j)
      r_idx[i] -= length + flen;
  }

  // copy the first half data, including prefix
  memcpy(old->data + old->pre, new->data + new->pre, length);
  old->keys = left;
  old->off  = length + old->pre;
  // update `old` index
  l_idx = node_index(old);
  r_idx = node_index(new);
  memcpy(l_idx, r_idx, left * index_byte);

  // update `length` with fence key length
  length += flen;
  // adjust `new` layout
  new->keys = right;
  new->off -= length;
  memmove(new->data + new->pre, new->data + new->pre + length, new->off - new->pre);

  // update node link
  new->next = old->next;
  old->next = new;
}

/****** BATCH operation ******/

#define get_op(n, off) ((uint32_t)(*(uint8_t *)(get_ptr(n, off) - sizeof(uint8_t))))

batch* new_batch()
{
  return new_node(Leaf, 0);
}

void free_batch(batch *b)
{
  free((void *)b);
}

void batch_clear(batch *b)
{
  b->keys = 0;
  b->off  = 0;
}

// insert a kv into node, this function allows duplicate key
static int batch_write(batch *b, uint32_t op, const void *key1, uint32_t len1, const void *val)
{
  int low = 0, high = (int)b->keys - 1;
  index_t *index = node_index(b);

  while (low <= high) {
    int mid = (low + high) / 2;

    get_key_info(b, index[mid], key2, len2);

    int r = compare_key(key2, len2, key1, len1);
    if (r <= 0)
      low  = mid + 1;
    else
      high = mid - 1;
  }

  --index;

  // check if there is enough space
  if ((char *)b->data + (b->off + sizeof(uint8_t) /* op */ + key_byte + len1 + value_bytes) > (char *)index)
    return -1;

  // set op type before kv
  *((uint8_t *)(b->data + b->off)) = (uint8_t)op;
  b->off += sizeof(uint8_t);

  if (low) memmove(&index[0], &index[1], low * index_byte);
  index[low] = b->off;

  node_insert_kv(b, key1, len1, val);

  return 1;
}

int batch_add_write(batch *b, const void *key, uint32_t len, const void *val)
{
  return batch_write(b, Write, key, len, val);
}

int batch_add_read(batch *b, const void *key, uint32_t len)
{
  return batch_write(b, Read, key, len, 0);
}

// read a kv at index
int batch_read_at(batch *b, uint32_t idx, uint32_t *op, void **key, uint32_t *len, void **val)
{
  // TODO: remove this
  if (idx >= b->keys) return 0;
  index_t *index = node_index(b);
  get_kv_ptr(b, index[idx], k, l, v);
  *op = get_op(b, index[idx]);
  *key = (void *)k;
  *len = l;
  *val = (void *)v;
  return 1;
}

void path_clear(path *p)
{
  p->depth = 0;
}

void path_set_kv_id(path *p, uint32_t id)
{
  p->id = id;
}

uint32_t path_get_kv_id(path *p)
{
  return p->id;
}

void path_push_node(path *p, node *n)
{
  assert(p->depth < max_descend_depth);
  p->nodes[p->depth++] = n;
}

node* path_get_node_at_level(path *p, uint32_t level)
{
  // TODO: remove this
  assert(p->depth > level);
  return p->nodes[p->depth - level - 1];
}

#ifdef Test

#include <stdio.h>

uint32_t get_node_size()
{
  return node_size;
}

static char* format_kv(char *ptr, char *end, node* n, uint32_t off)
{
  ptr += snprintf(ptr, end - ptr, "%u  ", off);
  uint32_t len = get_len(n, off);
  snprintf(ptr, len + 1, "%s", get_key(n, off));
  ptr += len;
  ptr += snprintf(ptr, end - ptr, " %llu\n", (val_t)get_val(n, off));
  return ptr;
}

void print_node(node *n, int detail)
{
  assert(n);
  int size = (float)node_size * 1.5;
  char buf[size], *ptr = buf, *end = buf + size;

  ptr += snprintf(ptr, end - ptr, "id: %u  ", n->id);
  ptr += snprintf(ptr, end - ptr, "type: %s  ",
    n->type == Root ? "root" : n->type == Branch ? "branch" : "leaf");
  ptr += snprintf(ptr, end - ptr, "level: %u  ", n->level);
  ptr += snprintf(ptr, end - ptr, "keys: %u  ", n->keys);
  snprintf(ptr, n->pre + 9, "prefix: %s", n->data);
  ptr += n->pre + 8;
  ptr += snprintf(ptr, end - ptr, "  offset: %u\n", n->off);

  index_t *index = node_index(n);
  if (detail) {
    for (uint32_t i = 0; i < n->keys; ++i)
      ptr = format_kv(ptr, end, n, index[i]);
  } else {
    if (n->keys > 0)
      ptr = format_kv(ptr, end, n, index[0]);
    if (n->keys > 1)
      ptr = format_kv(ptr, end, n, index[n->keys - 1]);
  }

  printf("%s\n", buf);
}

void print_batch(batch *b, int detail)
{
  assert(b);
  int size = (float)node_size * 1.5;
  char buf[size], *ptr = buf, *end = buf + size;

  ptr += snprintf(ptr, end - ptr, "keys: %u  ", b->keys);
  ptr += snprintf(ptr, end - ptr, "  offset: %u\n", b->off);

  index_t *index = node_index(b);
  if (detail) {
    for (uint32_t i = 0; i < b->keys; ++i) {
      ptr += snprintf(ptr, end - ptr, "%s ", get_op(b, index[i]) == Write ? "w" : "r");
      ptr = format_kv(ptr, end, b, index[i]);
    }
  } else {
    if (b->keys > 0) {
      ptr += snprintf(ptr, end - ptr, "%s ", get_op(b, index[0]) == Write ? "w" : "r");
      ptr = format_kv(ptr, end, b, index[0]);
    }
    if (b->keys > 1) {
      ptr += snprintf(ptr, end - ptr, "%s ", get_op(b, index[b->keys - 1]) == Write ? "w" : "r");
      ptr = format_kv(ptr, end, b, index[b->keys - 1]);
    }
  }

  printf("%s\n", buf);
}

// verify that all keys in node are in ascending order
static void validate(node *n, int is_batch)
{
  assert(n);

  if (n->keys == 0) return ;

  index_t *index = node_index(n);
  char *pre_key = get_key(n, index[0]);
  uint32_t pre_len = get_len(n, index[0]);

  for (uint32_t i = 1; i < n->keys; ++i) {
    char *cur_key = get_key(n, index[i]);
    uint32_t cur_len = get_len(n, index[i]);
    if (is_batch == 0)
      assert(compare_key(pre_key, pre_len, cur_key, cur_len) < 0);
    else
      assert(compare_key(pre_key, pre_len, cur_key, cur_len) <= 0);
    pre_key = cur_key;
    pre_len = cur_len;
  }
}

void node_validate(node *n)
{
  validate(n, 0);
}

void batch_validate(batch *n)
{
  validate(n, 1);
}

#endif /* Test */
