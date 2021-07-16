package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    TSDBClientConfig config;

    public ClientChannelInitializer(TSDBClientConfig config) {
        this.config = config;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {

    }
}
