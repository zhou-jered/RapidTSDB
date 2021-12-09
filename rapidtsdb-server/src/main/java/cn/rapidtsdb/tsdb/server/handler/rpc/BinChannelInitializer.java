package cn.rapidtsdb.tsdb.server.handler.rpc;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;

public class BinChannelInitializer extends ChannelInitializer<NioSocketChannel> {

    public BinChannelInitializer() {

    }

    @Override
    protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new ProtocolInitializerHandler());
    }
}
