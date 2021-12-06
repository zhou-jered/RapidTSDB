package cn.rapidtsdb.tsdb.client.handler;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(
            SocketChannel ch) {
        ch.pipeline().addLast(new VersionProtocolHandler());
    }
}
