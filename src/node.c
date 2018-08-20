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

#define get_len(n, off) ((uint32_t)(*(uint8_t *)((char *)n->data + off)))
#define get_key(n, off) ((char *)n->data + off + 1 /* length_byte */)
#define get_val(n, off) ((void *)(*(uint64_t *)(get_key(n, off) + get_len(n, off))))
#define node_index(n)   ((uint16_t *)((char *)n + (node_size - n->keys * 2 /* index_bytes */)))

void set_node_size(uint32_t size)
{
	node_size = size < node_min_size ? node_min_size : size > node_max_size ? node_max_size : size;
	node_size &= node_size_mask;
}

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

	uint16_t *index = node_index(n);
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

		const void *key2 = get_key(n, index[mid]);
		uint32_t len2 = get_len(n, index[mid]);

		if (compare_key(key2, len2, key1, len1) < 0)
			low  = mid;
		else
			high = mid - 1;
	}

	if (compare_key(get_key(n, index[low]), get_len(n, index[low]), key1, len1) < 0)
		return (node *)get_val(n, index[low]);
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
	uint16_t *index = node_index(n);
	const void *key1 = key + n->pre;
	uint32_t    len1 = len - n->pre;

	while (low <= high) {
		int mid = (low + high) / 2;

		const void *key2 = get_key(n, index[mid]);
		uint32_t len2 = get_len(n, index[mid]);

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
		uint16_t *index = node_index(n);
		const void *key1 = key + n->pre;
		uint32_t    len1 = len - n->pre;

		while (low <= high) {
			int mid = (low + high) / 2;

			const void *key2 = get_key(n, index[mid]);
			uint32_t len2 = get_len(n, index[mid]);

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
	uint16_t *index = node_index(n) - 1;

	// check if there is enough space
	if ((char *)n->data + (n->off + 1 /* length_byte */ + klen + sizeof(uint64_t *)) > (char *)index)
		return -1;

	if (pos) memmove(&index[0], &index[1], pos * 2 /* index_bytes */);
	index[pos] = n->off;

	*((uint8_t *)n->data + n->off) = (uint8_t)klen;
	n->off += 1;
	memcpy(n->data + n->off, key + n->pre, klen);
	n->off += klen;
	if (val)
		*((uint64_t *)(n->data + n->off)) = *(uint64_t *)(&val);
	else
		memset(n->data + n->off, 0, sizeof(uint64_t *));
	n->off += sizeof(uint64_t *);

	++n->keys;

	return 1;
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
	snprintf(ptr, n->pre + 9, "prefix: %s", n->pre ? n->data : "null");
	ptr += n->pre + 8;
	ptr += snprintf(ptr, end - ptr, "  offset: %u\n", n->off);

	uint16_t *index = node_index(n);
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

// varify that all keys in node are in ascending order
void node_validate(node *n)
{
	assert(n);

	if (n->keys == 0) return ;

	uint16_t *index = node_index(n);
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

uint32_t node_get_key_count(node *n)
{
	assert(n);
	return n->keys;
}

#endif /* Test */
