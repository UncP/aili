/**
 *    author:     UncP
 *    date:    2018-08-22
 *    license:    BSD-3
**/

#ifndef _palm_h_
#define _palm_h_

#include "node.h"

// batch is a wrapper for node
// it accepts kv pairs and divide them into different threads for further process
typedef node batch;

batch* new_batch();
void free_batch();

#endif /* _palm_h_ */