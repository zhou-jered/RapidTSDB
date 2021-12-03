package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    ClientSession clientSession;

    public ClientChannelInitializer(
            ClientSession clientSession) {
        this.clientSession = clientSession;
    }

    @Override
    protected void initChannel(
            SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new VersionProtocolHandler());
    }
}
