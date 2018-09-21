# aili

[![Author](https://img.shields.io/badge/Author-UncP-brightgreen.svg)](https://github.com/UncP)
[![Version](https://img.shields.io/badge/Version-1.1.0-blue.svg)](https://github.com/UncP/aili)

#### Algorithms
* Palm Tree : Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors, [Paper](http://www.vldb.org/pvldb/vol4/p795-sewall.pdf)


#### 版本信息
| 版本 |           备注             |
|:------:|:---------------------------:|
| [1.0.0](https://github.com/UncP/aili/tree/1.0.0)  | Palm Tree |
| [1.1.0](https://github.com/UncP/aili/tree/1.1.0)  | Palm Tree with point-to-point synchronization |


##### Have a try
```bash
1. make third_party
                                       # key_num  key_len  file_name
2. make generate_data && ./generate_data 10000000    10       0      # test data will be in ./data directory
                                           # file_name  thread_num  queue_num  key_num
3. make palm_tree_test && ./palm_tree_test     0           3            8      10000000
# if your machine supports 4 threads, thread_num can be 1, 2, 3, not 4
```
