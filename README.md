# 简介
English Version: [English Version](/README_EN.md)
一个时序数据库，提供非常快速的时序数据插入和查询，同时提供丰富的数据聚合功能。
实现了论文 [Gorilla: A Fast, Scalable, In-Memory Time Series DataBase](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)

# 特点
- 内存存储数据，读写非常高效快速。
- 惊人的高压缩率的数据压缩，相比传统数据库，可节省20倍以上存储空间。
- 稳定的，失败恢复机制
- 灵活的自定义持久化存储
- openTSDB http 接口兼容
- 高效的 rpc 通信协议


# 架构图 （暂时）
相关的概念可以参考论文
![Architecture](https://github.com/zhou-jered/RapidTSDB/raw/master/docs/images/TSBlock%20Management.jpg)


# 模块功能

### AOL Manager
Append Only Log 管理器，在写入数据的同时写一份Log，Append Only，每64KB 刷新一次到磁盘。同时作为失败恢复的Log。
所以理论上，你最多只会丢失64KB的数据，参考了论文里的大小设置。
每一个 Log Entry 会有一个自增的Long型 ID。每次写入操作产生一个AOLog。

### CheckPoint Manger
每两小时刷新一次内存数据到磁盘，同时将最新的 AOLog ID 的作为checkpoint存储下来

### Metric key Manager
分配Metric Name 到 int ID 的映射和查询管理器。

### StoreHandler
持久化存储的接口协议，通过不同的实现，可以将数据存储在任何你希望的地方，文件，Hadoop，分布式文件系统，
Hbase，S3。。。从而实现了存储和计算分离。


