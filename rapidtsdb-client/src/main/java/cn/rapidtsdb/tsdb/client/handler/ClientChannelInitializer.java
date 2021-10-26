package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.TSDBClientSession;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    TSDBClientSession clientSession;

    public ClientChannelInitializer(TSDBClientSession clientSession) {
        this.clientSession = clientSession;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new VersionProtocolHandler());
    }
}
