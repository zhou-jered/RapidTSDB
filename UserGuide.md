# User Guide


## Deploy in Standalone with local file storage
Standalone Mode with a single server node with 4 Core CPU, 8G memory 
can serve about 1 millions metrics in good performance.

## Deploy in Standalone with Hbase/Hadoop/S3... storage
If you choice another storage system, it means that you have
thrown some calculation and memory storage requirement to another system,
which means you can provide more memory space and calculation resources in time series
data calculation, while the cost is the network communication time increment.
Store data in third party storage system can make sure you data will hardly loss.
Based on our test, store data in hbase with about 10ms network time cost.
TSDB system todo.... 

## Cluster Mode
