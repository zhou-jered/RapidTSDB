package cn.rapidtsdb.tsdb.server;

import cn.rapidtsdb.tsdb.server.handler.CommonStringWriteHandler;
import cn.rapidtsdb.tsdb.server.handler.ProtocolHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class TSDBChannelInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ProtocolHandler());
        ch.pipeline().addFirst(new CommonStringWriteHandler());
    }
}
