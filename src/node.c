/**
 *    author:     UncP
 *    date:    2018-08-19
 *    license:    BSD-3
**/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "node.h"

#define node_min_size (((uint32_t)1) << 12) // 4kb
#define node_max_size (((uint32_t)1) << 20) // 1mb

static uint32_t node_size = node_min_size;

static uint32_t get_len(node *n, uint16_t off)
{
	return *(uint8_t *)((char *)n->data + off);
}

static inline const void* get_key(node *n, uint16_t off)
{
	return (const void *)((char *)n->data + off + 1 /* length_byte */);
}

static inline void* get_val(node *n, uint16_t off)
{
	return (void *)(get_key(n, off) + get_len(n, off));
}

static inline uint16_t* node_index(node *n)
{
	return (uint16_t *)((char *)n + (node_size - n->keys * 2 /* index_bytes */));
}

static int compare_key(const void *key1, uint32_t len1, const void *key2, uint32_t len2)
{
	uint32_t min = len1 < len2 ? len1 : len2;
	int r = memcmp(key1, key2, min);
	return r ? r : (len1 == len2 ? 0 : (len1 < len2 ? 1 : -1));
}

node* new_node(uint8_t type, uint8_t level)
{
	node* n  = (node *)malloc(node_size);
	n->type  = type;
	n->level = level;
	n->keys  = 0;
	n->pre   = 0;
	n->off   = 0;
	n->next  = 0;
	n->first = 0;

	return n;
}

void free_node(node *n)
{
	free((void *)n);
}

node* descend(node *n, const void *key, uint32_t len)
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

// find the key in the leaf, return its val, if no such key, return null
void* search(node *n, const void *key, uint32_t len)
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

int insert(node *n, const void *key, uint32_t len, const void *val)
{
	int pos = -1;

	if (n->pre) { // compare with node prefix
		int r = compare_key(key, len, n->data, n->pre);
		if (r < 0)
			pos = 0;
		else if (r > 0)
			pos = n->keys;
	}

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

	uint16_t *index = node_index(n) - 1;
	if (pos) memmove(&index[0], &index[1], pos * 2 /* index_bytes */);
	index[pos] = n->off;

	uint32_t klen = len - n->pre;
	*((uint8_t *)n->data + n->off) = (uint8_t)klen;
	n->off += 1;
	memcpy(n->data + n->off, key + n->pre, klen);
	n->off += klen;
	*((uint64_t *)(n->data + n->off)) = *(uint64_t *)val;
	n->off += sizeof(uint64_t *);

	++n->keys;

	return 1;
}

void print_node(node *n, int detail)
{
	int size = node_size * 2;
	char buf[size], *ptr = buf;

	int off = 0;
	off += snprintf(ptr + off, size - off, "type: %s  ", n->type == Root ? "root" : n->type == Branch ? "branch" : "leaf");
	off += snprintf(ptr + off, size - off, "level: %u  ", n->level);
	off += snprintf(ptr + off, size - off, "keys: %u  ", n->keys);
	snprintf(ptr + off, n->pre + 1, "prefix: %s  ", n->data);
	off += n->pre + 10;
	off += snprintf(ptr + off, size - off, "offset: %u\n", n->off);

	uint16_t *index = node_index(n);
	if (detail) {
		for (uint32_t i = 0; i < n->keys; ++i) {
			uint32_t len = get_len(n, index[i]);
			snprintf(ptr + off, len + 2, "%s ", get_key(n, index[i]));
			off += len + 1;
			off += snprintf(ptr + off, size - off, "%llu\n", *(uint64_t *)get_val(n, index[i]));
		}
	} else {
		if (n->keys > 0) {
			uint32_t len = get_len(n, index[0]);
			snprintf(ptr + off, len + 2, "%s ", get_key(n, index[0]));
			off += len + 1;
			off += snprintf(ptr + off, size - off, "%llu\n", *(uint64_t *)get_val(n, index[0]));
		}
		if (n->keys > 1) {
			uint32_t len = get_len(n, index[n->keys - 1]);
			snprintf(ptr + off, len + 2, "%s ", get_key(n, index[n->keys - 1]));
			off += len + 1;
			off += snprintf(ptr + off, size - off, "%llu\n", *(uint64_t *)get_val(n, index[n->keys - 1]));
		}
	}

	printf("%s\n", buf);
}
