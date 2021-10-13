package cn.rapidtsdb.tsdb.server.handler.rpc.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class StringWriterHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof String) {
            String str = (String) msg;
            ByteBuf byteBuf = ctx.alloc().buffer(str.length());
            byteBuf.writeBytes(str.getBytes());
            ctx.writeAndFlush(byteBuf);
        }
    }
}
