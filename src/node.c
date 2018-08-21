/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "node.h"

#define node_size_mask ((uint64_t)~0xfff)

static uint32_t node_size = node_min_size;

void set_node_size(uint32_t size)
{
	node_size = size < node_min_size ? node_min_size : size > node_max_size ? node_max_size : size;
	node_size &= node_size_mask;
}

#define get_len(n, off) ((uint32_t)(*(len_t *)((char *)n->data + off)))
#define get_key(n, off) ((char *)n->data + off + key_byte)
#define get_val(n, off) ((void *)(*(uint64_t *)(get_key(n, off) + get_len(n, off))))
#define node_index(n)   ((index_t *)((char *)n + (node_size - n->keys * index_byte)))
#define get_key_info(n, off, key, len) \
  const void *key = get_key(n, off);   \
  uint32_t len = get_len(n, off);
#define get_kv_info(n, off, key, len, val) \
  const void *key = get_key(n, off);       \
  uint32_t len = get_len(n, off);          \
  void *val = get_val(n, off);

static int compare_key(const void *key1, uint32_t len1, const void *key2, uint32_t len2)
{
	uint32_t min = len1 < len2 ? len1 : len2;
	int r = memcmp(key1, key2, min);
	return r ? r : (len1 == len2 ? 0 : (len1 < len2 ? 1 : -1));
}

static uint32_t node_id = 0;

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
	assert(n->type != Leaf && n->keys);

	index_t *index = node_index(n);
	int low = 0, high = (int)n->keys - 1, mid;

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
		mid = (low + high + 1) / 2;

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
	assert(n->type == Leaf);

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
	*((len_t *)n->data + n->off) = (len_t)len;
	n->off += key_byte;
	memcpy(n->data + n->off, key, len);
	n->off += len;
	if (val)
		*((uint64_t *)(n->data + n->off)) = *(uint64_t *)(&val);
	else
		*((uint64_t *)(n->data + n->off)) = 0;
	n->off += value_bytes;

	++n->keys;
}

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
			if (r == 0) {
				// key already exists
				return 0;
			} else if (r < 0) {
				low  = mid + 1;
			} else {
				high = mid - 1;
			}
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
	}

	// we first copy all the keys to `new` in sequential order,
	// then move the first half back to `old` and adjust the other half in `new`
	uint32_t length = 0;
	r_idx -= old->keys;
	for (uint32_t i = 0; i < old->keys; ++i) {
		r_idx[i] = new->off;
		get_kv_info(old, l_idx[i], okey, olen, oval);
		node_insert_kv(new, okey, olen, oval);
		if (i == left - 1)
			length = new->off - new->pre;
	}

	// copy the first half data, including prefix
	memcpy(old->data + old->pre, new->data + new->pre, length);
	old->keys = left;
	old->off  = length + old->pre;
	// update `old` index
	l_idx = node_index(old);
	r_idx = node_index(new);
	memcpy(l_idx, r_idx, left * index_byte);

	// ignore the fence key if level > 0 since it will be promoted
	if (old->level)
		length += key_byte + flen + value_bytes;

	// adjust `new` layout
	new->keys = right;
	new->off -= length;
	memmove(new->data + new->pre, new->data + new->pre + length, new->off - new->pre);
	r_idx = node_index(new);
	// adjust `new` index
	for (uint32_t i = 0; i < new->keys; ++i)
		r_idx[i] -= length;

	// update node link
	new->next = old->next;
	old->next = new;
}

#ifdef Test

#include <stdio.h>

uint32_t get_node_size()
{
	return node_size;
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
		for (uint32_t i = 0; i < n->keys; ++i) {
			uint32_t len = get_len(n, index[i]);
			snprintf(ptr, len + 1, "%s", get_key(n, index[i]));
			ptr += len;
			ptr += snprintf(ptr, end - ptr, " %llu\n", (uint64_t)get_val(n, index[i]));
		}
	} else {
		if (n->keys > 0) {
			uint32_t len = get_len(n, index[0]);
			snprintf(ptr, len + 1, "%s", get_key(n, index[0]));
			ptr += len;
			ptr += snprintf(ptr, end - ptr, " %llu\n", (uint64_t)get_val(n, index[0]));
		}
		if (n->keys > 1) {
			uint32_t len = get_len(n, index[n->keys - 1]);
			snprintf(ptr, len + 1, "%s", get_key(n, index[n->keys - 1]));
			ptr += len;
			ptr += snprintf(ptr, end - ptr, " %llu\n", (uint64_t)get_val(n, index[n->keys - 1]));
		}
	}

	printf("%s\n", buf);
}

// verify that all keys in node are in ascending order
void node_validate(node *n)
{
	assert(n);

	if (n->keys == 0) return ;

	index_t *index = node_index(n);
	char *pre_key = get_key(n, index[0]);
	uint32_t pre_len = get_len(n, index[0]);

	for (uint32_t i = 1; i < n->keys; ++i) {
		char *cur_key = get_key(n, index[i]);
		uint32_t cur_len = get_len(n, index[i]);
		assert(compare_key(pre_key, pre_len, cur_key, cur_len) < 0);
		pre_key = cur_key;
		pre_len = cur_len;
	}
}

#endif /* Test */
