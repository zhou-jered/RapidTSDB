# 概述
由于Core模块仅能提供类似 metric name 映射到 time series data 的数据结构，为了支持带 tag 的时序数据写入和读取，
需要引入一个中间层模块做转换，转换从 metric with tag 到单个 metric name的转换和逆向转换。
除了简单的转换，还需要扫描一个 metric 的 tag 列表， tag 的值列表。

# 架构图
![Architecture](https://github.com/zhou-jered/RapidTSDB/raw/master/docs/images/meta-middle.jpg)

# 转换原理
字典树存储，扫描即可
使用特殊字符分割 metric name 与 tag value。
同时为了减少转换后的 metric name 字符长度，参考 openTSDB 的做法，将 tag key 和 tag value 映射到
一个数字ID，拼接 tag key 和 value 的 ID 即可，同时维护 tag key 和 value 到 ID 的双向转换关系。
openTSDB 为了减少在扫描数据时候的性能消耗，将 tag key 的数量限制在8个，但是我们这比较块，就可以将这个限制放开一点，
可以开放到64个 tag key。为什么是 64 个？<del>拍脑门想的。 （不是）</del>，结合实际的业务，64个tag能够满足绝大多数的业务需求。
