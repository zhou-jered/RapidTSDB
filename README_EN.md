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
Append Only Log Manager，while writing a point of time series data, An entry of record log will also be written in AOLog file buffer
, flush to persist stoage each 64KB。When application restart, recovery data is based on these log.
Each entry of log had a long type id, used to record as the offset of the log.
As the paper suggested, Maximun buffer size is 64KB, it is also means that in the worst case, a maximun data
of 64KB could be lost. But don't worry about this, in most case, you won't lost any data.  

### CheckPoint Manger
When the memory time series data flush to persist storage, record the aolog offset/index as the checkpoint.
when application restart, we know that how much data are already been stored safely, and how much data are in
the fly and in the log.

### Metric key Manager
Using to manage the mapping relation between the metric text and the metric ID.

### StoreHandler
The interface of the persistent storage, you can implement it to store the data whereever you want,
such as Hadoop, HDFS, HBASE, S3, etc.
Storage and Computation are separated in design.