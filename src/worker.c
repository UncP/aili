/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#include <assert.h>

#include "worker.h"

worker* new_worker(uint32_t id, uint32_t total, barrier *b)
{
	assert(id < total);

	worker *w = (worker *)malloc(sizeof(worker));
	w->id = id;
	w->total = total;
	w->bar = b;
	w->max_num = 64;
	w->cur_num = 0;
	w->paths = (path *)malloc(sizeof(path) * w->max_num);

	return w;
}

void free_worker(worker* w)
{
	free((void *)w->paths);
	free((void *)w);
}

path* worker_get_new_path(worker *w)
{
	if (w->cur_num == w->max_num) {
		w->max_num = (uint32_t)((float)w->max_num * 1.5);
		w->paths = (path *)realloc(w->paths, sizeof(path) * w->max_num);
	}
	assert(w->cur_num < w->max_num);
	return w->paths[w->cur_num++];
}

void worker_clear(worker *w)
{
	for (uint32_t i = 0; i < w->cur_num; ++i)
		path_clear(&w->paths[i]);
	w->cur_num = 0;
}
