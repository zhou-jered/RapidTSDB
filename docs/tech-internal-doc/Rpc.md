# Server 的运行模式

## 协议动态切换
在以下条件满足的时候，切换到 Human readable 的 console模式
连接创建后

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


# Rpc 模式
在创建连接的三秒内，服务器接收到rpc magic number即切换到二进制协议模式。
切换到二进制协议模式后，如果配置有认证，需要提交认证信息后，才能提交执行命令。
无认证模式可直接提交命令。


