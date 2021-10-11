package cn.rapidtsdb.tsdb.server.handler.console;

import cn.rapidtsdb.tsdb.server.handler.rpc.ProtocolHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;

public class TSDBChannelInitializer extends ChannelInitializer<NioSocketChannel> {
    @Override
    protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ProtocolHandler());
        ch.pipeline().addFirst(new CommonStringWriteHandler());
    }
}
