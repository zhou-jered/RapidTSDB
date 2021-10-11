package cn.rapidtsdb.tsdb.server.handler.console;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;

public class ConsoleChannelInitializer extends ChannelInitializer<NioSocketChannel> {
    @Override
    protected void initChannel(NioSocketChannel ch) throws Exception {
        ch.pipeline().addLast(new LineBasedFrameDecoder(2048));
        ch.pipeline().addLast(new ConsoleHandler());
        ch.pipeline().addFirst(new CommonStringWriteHandler());
    }
}
