# Server 的运行模式
Server监听端口，有连接接入的时候，默认进入Console Command 模式。
当客户端发送 MagicNumber时候进入到Rpc协议模式。
Console Command模式接手人类可读的 text command 作为执行命令。
关于Console Command模式的配置如下

|config|note|example
|-|-|-|
|server.console.enable|是否启用console command 模式||
|server.console.auth|console command 模式是否需要认证||
|server.console.token|认证所需的token||
Console Command 模式仅提供基于token的认证模式，该模式不应该成为生产环境的使用方式。


# Rpc 协议

在初始化连接的时候需要提交认证信息，连接认证完毕后可以提交方法执行命令。
### 连接
MagicNumber-0xbabcaffe|8bit-协议版本|8bit - AuthType|
Auth Data｜

### 方法执行
｜


## 写入数据
## 查询数据
## 连接认证
