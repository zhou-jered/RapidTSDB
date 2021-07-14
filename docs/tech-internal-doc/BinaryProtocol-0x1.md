# 概述
## 版本 1.0.0 0x1
客户端与服务端建立连接之后，连接会处于以下状态

|状态|说明|
|-|-|-|-|
|等待认证|连接创建初始状态|
|认证中|提交认证信息|
|可读写|读写数据交互|

## 响应码表
|响应码|说明|
|-|-|
|0x0| 操作成功|
|0xff|协议出错|
|0xee|服务端内部错误|
|0x10|客户端版本过低|
|0x20|客户端版本过高|
|0x30|服务端拒绝处理|
|0x31|不支持的downsampler|
|0x32|不支持的aggregator|


## 服务端命令码表
|命令码|说明|
|-|-|
|0x0|正常，继续发送数据|
|0xee|断开连接|

# 连接初始化(认证)

MagicNumber-0xbabcaffe|8bit-协议版本|8bit - AuthType|
Auth Data(Var length, depending on AuthType)|client minimum supported version(8bit)
| client maximum supported version(8bit)|

## 连接初始化（认证）响应
|8bit-协议版本|8bit-response code|data length in bytes (16 bit) |
|resp data| nextCommand(8bit) |

