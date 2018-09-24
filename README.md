# aili

[![Author](https://img.shields.io/badge/Author-UncP-brightgreen.svg)](https://github.com/UncP)
[![Version](https://img.shields.io/badge/Version-1.2.0-blue.svg)](https://github.com/UncP/aili)

#### References
* [Parallel Architecture-Friendly Latch-Free Modifications to B+ Trees on Many-Core Processors](http://www.vldb.org/pvldb/vol4/p795-sewall.pdf)
* [Prefix B-Trees](http://delivery.acm.org/10.1145/330000/320530/p11-bayer.pdf?ip=111.114.49.2&id=320530&acc=ACTIVE%20SERVICE&key=BF85BBA5741FDC6E%2E4510866D46BF76B7%2E4D4702B0C3E38B35%2E4D4702B0C3E38B35&__acm__=1537792786_42d3c27bf4ea064b8d68b89657e39bf6)
* [Efficient Locking for Concurrent Operations on B-Trees](https://www.csd.uoc.gr/~hy460/pdf/p650-lehman.pdf)

#### 版本信息
| 版本 |           备注             |
|:------:|:---------------------------:|
| [1.0.0](https://github.com/UncP/aili/tree/1.0.0)  | Palm Tree |
| [1.1.0](https://github.com/UncP/aili/tree/1.1.0)  | Palm Tree with point-to-point synchronization |
| [1.2.0](https://github.com/UncP/aili/tree/1.2.0)  | Prefix Palm Tree |


#### 试一下
```bash
1. make third_party
                                       # key_num  key_len  file_name
2. make generate_data && ./generate_data 10000000    10       0      # test data will be in ./data
                                           # file_name  thread_num  queue_num  key_num
3. make palm_tree_test && ./palm_tree_test     0           3            8      10000000

# if your machine supports N threads, thread_num can be 1, 2, ..., (N-1), not N
# key_num can be [1, infinity), depending on how many test keys you generate
```


#### 其他
+ 你可以关注我的[知乎专栏](https://zhuanlan.zhihu.com/b-tree)，这个仓库的相关内容会在知乎上面介绍
