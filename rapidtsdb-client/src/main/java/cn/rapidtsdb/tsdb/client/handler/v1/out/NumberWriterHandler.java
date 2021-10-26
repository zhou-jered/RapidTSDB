package cn.rapidtsdb.tsdb.client.handler.v1.out;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class NumberWriterHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Short) {
            ByteBuf byteBuf = ctx.alloc().buffer(2);
            byteBuf.writeShort((Short) msg);
        }
        if (msg instanceof Integer) {
            ByteBuf byteBuf = ctx.alloc().buffer(4);
            byteBuf.writeInt((Integer) msg);
        }
        if (msg instanceof Long) {
            ByteBuf byteBuf = ctx.alloc().buffer(8);
            byteBuf.writeLong((Long) msg);
        }
    }
}
