/**
 *    author:     UncP
 *    date:    2018-08-24
 *    license:    BSD-3
**/

#ifndef _worker_h_
#define _worker_h_

#include "node.h"
#include "barrier.h"

/**
 *   worker is actual thread that does write/read operations to palm tree.
**/
typedef struct worker
{
	uint32_t  id;       // my id
	uint32_t  total;    // total workers
	barrier  *bar;      // barrier of the worker pool
	uint32_t  max_num;  // maximum path number
	uint32_t  cur_num;  // current path number
	path     *paths;    // paths for all the keys this worker operates
}worker;

worker* new_worker(uint32_t id, uint32_t total, barrier *b);
void free_worker(worker* w);
path* worker_get_new_path(worker *w);
void worker_clear(worker *w);

#endif /* _worker_h_ */