package cn.rapidtsdb.tsdb.common.protonetty.out;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
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
        if (msg instanceof Integer) {
            byteBuf = ctx.alloc().buffer(4);
            byteBuf.writeInt((Integer) msg);
        }
        if (msg instanceof Long) {
            byteBuf = ctx.alloc().buffer(8);
            byteBuf.writeLong((Long) msg);
        }
        if (msg instanceof byte[]) {
            byteBuf = ctx.alloc().buffer(((byte[]) msg).length);
            byteBuf.writeBytes((byte[]) msg);
        }
        if (byteBuf != null) {
            ctx.writeAndFlush(byteBuf);
        } else {
            ctx.writeAndFlush(msg);
        }
        promise.setSuccess();
    }

}
