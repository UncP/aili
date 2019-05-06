### 4.1.0

**Multi-ART**

1. （BUG）节点扩展后没有获取新 version
2. （BUG）Node256 初始化 version 设置错误
3. （BUG）使用自己的内存分配器释放内存时调用了系统函数
4. （BUG）Node256 下降时没有判断节点是否为空
5. （BUG）b link tree 判断右移时没有使用旧节点的 fence key
6. 将 leaf 的 key 和 len 打包在一起
7. 带有随机数生成器的用于测试所有索引的 benchmark, one benchfuck to rule them all!
8. （BUG）替换叶子节点时没有考虑到父节点为空的情况
9. （BUG）节点加锁后解决前缀冲突时没有再次进行前缀检查
10. （BUG）加锁后发现前缀变化重试时没有解锁
11. （BUG）Palm Tree Worker 计算任务时出现 beg > end
12. （BUG）修复节点无法获取正确父节点的 bug
13. （BUG）修复节点扩展时对叶子节点设置父节点的 bug
14. （BUG）修复提前设置节点父节点的 bug
15. （BUG）修复下降到错误层级节点的 bug，通过记录节点开始比较的 key 偏移



### 4.0.0

**Adaptive Radix Tree**

1. 单线程 adaptive radix tree
2. （BUG）初始化根节点不为空
3. （BUG）节点扩展没有拷贝前缀
4. （BUG）叶子节点不支持256字节长度的 key，改为支持小于16字节
5. （BUG）节点前缀截断时没有按照实际长度来
6. （BUG）节点下降时没有获取正确的二级指针



### 3.2.0

**Mass Tree 读优化**

1. 解决潜在的 `mass_tree_get` bug，当确认获取的 key 信息安全（节点没有发生插入或分裂）后才进行处理
2. 解决生成测试数据的 bug
3. 重构节点的 `permutation`
4. 以更安全的方式设置节点的父节点，简化代码
5. （BUG）border node 分裂时没有修改其 subtree 的父节点



### 3.1.0

**Mass Tree 移除多余的 lower key**

1. 优化 keyslice 获取，移除重试时 keyslice 的获取
2. 去掉 border node 的 lower key，因为 lower key 始终位于 index 0 处，不需要多余的 lower key
3. 完善测试数据的生成，添加测试脚本



### 3.0.0

**Mass Tree**

1. 改进版的 B<sup>link</sup> Tree
2. （BUG）border 节点分裂时没有 fence key 截断，对应的，interior 节点下降时只有 keyslice 相等时，才能进行偏移的更新
3. （BUG）key 偏移更新问题。第二个 bug 是错的，只有当下降到 subtree 时，才需要进行 key 偏移的更新，同时只有当生成 subtree 时才需要进行截断
4. （BUG）节点下降时根据错误的映射关系获取了错误的节点
5. （BUG）获取节点信息的宏出现错误，没有更安全地加上括号
6. （BUG）分裂后将新节点插入父节点时，同时设置父节点，但如果父节点此时也分裂了，可能造成父节点设置错误
7. （BUG）生成 subtree 后没有解锁节点
8. （BUG）替换节点时没有修改指示位
9. （BUG）获取冲突 key 长度时指针使用错误，造成 segment fault
10. （BUG）生成 subtree 时没有考虑到 keyslice 冲突的问题，通过生成多层 subtree 并且懒惰生成的方式解决
11. （BUG）替换 subtree 时没有设置 subtree 的父节点
12. （BUG）生成 subtree 时没有连接所有节点
13. （BUG）搜索节点时没有正确处理中间状态（unstable state）
14. （BUG）（13衍生）获取 key 偏移时错误理解小端字节序
15. （FIX）解决 border node 的 lower key 问题，但是降低了性能，同时增加了内存使用。A necessary evil
16. （BUG）border node 的 lower key 选取错误，应该选择分裂时新节点的第一个 key，而不是旧节点的最后一个 key
17. 无论是顺序插入还是随机插入，性能出乎意料得好



### 2.3.0

**将 level 0 节点从1/2满提高到2/3，即 B<sup>*</sup> Tree**
1. 更紧凑的节点布局，大幅节省节点内存。
2. 由于这个优化会带来新的 cache miss，并且需要额外的一些 CPU 计算，所以预计并不会对 palm tree 性能提升有帮助，甚至有可能下降？
3. 如果把当前节点的 key 挪过去了，那当前 worker 后续的 key 可能需要重新定向。（发现这个问题之前就解决了）
4. 也可能挪过去了会造成后续 key 插入时节点的 key 挪动，不过也可能可以避免当前节点后续 key 的移动，所以并不是一个问题。
5. 从之前的测试情况来看，4k节点从设计上来说是有问题的，因为瓶颈在 cache miss 上，而且4k节点在实现 b<sup>*</sup>node 时也带来了很大的问题，需要很多 CPU 计算。所以需要重新设计节点的结构，核心的改变是以 CACHE LINE(64 字节) 为单位，希望获得更好的 cache locality。
6. 这个优化和 Prefix B Tree 是冲突的
7. （BUG）单个节点发生多次 move 导致多个 key 需要被替换，会导致在 level 1 替换 key 时发现有的 key 不存在。所以需要进行 key 过滤
8. （BUG）两个节点同时往新节点移入 key 时没有修改 `next` 域
9. （BUG）右移时顺序错误
10. （BUG）统计可移入字节数时只统计了 key 的长度，没有统计其他域
11. （BUG）由于 key 的左移，导致无法只通过右移来确定实际插入的 level 0 节点
12. （BUG）顺序插入优化时没有连接新节点
13. 移除了顺序插入优化的标记 `sopt`
14. （BUG）当 replace fence key 到达父节点时，父节点已经发生了分裂，导致要被替换的 key 被提升到了父节点上层

**总结：**

* 随机插入性能稍有下降（<5%），顺序插入性能不受影响
* 节点占用内存减少约13%，但是目前没有优化彻底，所以没有达到预期效果，具体情况需要等待后续完成 palm tree 信息统计后才可以得知


### 2.2.0

**对 level 0 进行前缀压缩（Prefix B Tree）**

1. 为了简化编程暂不考虑 key 长度小于等于前缀的情况。
2. 对于写入时 prefix 不一致的问题，需要新分配节点来插入 key，是否会造成很多零碎节点？会有但并不是问题。
3. 优化效果十分良好，对于100万个10字节 key 会有8%以上的内存节省，随着 key 数量的增长内存节省会更明显。



### 2.1.0

**节点内存分配器**

1. 每个线程对应一个分配器，尽可能减少内存分配毛刺的出现，更加稳定。
2. 因为测试线程少，所以目前分配器并不会带来什么实际效果。
3. 是否可以考虑和 prefetch 相结合？
4. 是否可以引入新的以线程为单位的节点布局（CSB+ Tree）？
