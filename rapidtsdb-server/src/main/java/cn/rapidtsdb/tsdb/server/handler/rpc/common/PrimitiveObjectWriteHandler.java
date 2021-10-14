package cn.rapidtsdb.tsdb.server.handler.rpc.common;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class PrimitiveObjectWriteHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ByteBuf byteBuf = null;
        if (msg instanceof String) {
            String str = (String) msg;
            byteBuf = ctx.alloc().buffer(str.length());
            byteBuf.writeBytes(str.getBytes());
        }
        if (msg instanceof Short) {
            byteBuf = ctx.alloc().buffer(2);
            byteBuf.writeShort((Short) msg);
        }
        if (msg instanceof Short) {
            byteBuf = ctx.alloc().buffer(2);
            byteBuf.writeShort((Short) msg);
        }
        if (msg instanceof Integer) {
            byteBuf = ctx.alloc().buffer(4);
            byteBuf.writeInt((Integer) msg);
        }
        if (msg instanceof Long) {
            byteBuf = ctx.alloc().buffer(8);
            byteBuf.writeLong((Long) msg);
        }
        if (byteBuf != null) {
            ctx.writeAndFlush(byteBuf);
        }
    }

}
