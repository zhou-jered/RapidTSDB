# Overview
中文: [Chinese Version](/README.md)
A Time Series Database，Providing very fast TS data insert and query service, with
many useful aggregation function.
This Repo is an implementation of the paper: [Gorilla: A Fast, Scalable, In-Memory Time Series DataBase](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)

# Feature
- Memory Store
- Very high Ratio data compression
- Fault Tolerance
- Flexible Persistence Storage
- Compatible with the openTSDB HTTP API
- High performance Rpc protocol&implementation


# Architecture （Temporarily）
For any DESIGN information about the module, see the paper.
![Architecture](https://github.com/zhou-jered/RapidTSDB/raw/master/docs/images/TSBlock%20Management.jpg)


# Module

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