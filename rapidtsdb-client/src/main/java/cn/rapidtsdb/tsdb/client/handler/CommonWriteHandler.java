package cn.rapidtsdb.tsdb.client.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class CommonWriteHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof byte[]) {
            byte[] content = (byte[]) msg;
            ByteBuf byteBuf = ctx.alloc().buffer(content.length);
            byteBuf.writeBytes(content);
            ctx.write(byteBuf, promise);
        }
    }
}
