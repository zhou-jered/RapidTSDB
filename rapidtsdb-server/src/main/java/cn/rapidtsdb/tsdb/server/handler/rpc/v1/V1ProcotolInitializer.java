package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;

public class V1ProcotolInitializer extends ChannelInitializer<NioSocketChannel> {
    @Override
    protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ProtocolDecodeHandler(),
                new AuthHandler(),
                new CommandDispatcherHandler());
    }
}
