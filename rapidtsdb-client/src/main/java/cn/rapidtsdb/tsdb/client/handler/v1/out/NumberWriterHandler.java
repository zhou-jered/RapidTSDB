package cn.rapidtsdb.tsdb.client.handler.v1.out;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class NumberWriterHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        ByteBuf byteBuf = null;

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
        if (msg instanceof String) {
            byteBuf = ctx.alloc().buffer(((String) msg).length());
            byteBuf.writeBytes(((String) msg).getBytes());
        }
        if (byteBuf != null) {
            ctx.writeAndFlush(byteBuf);
        } else {
            ctx.writeAndFlush(msg);
        }
        promise.setSuccess();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        super.flush(ctx);
    }
}
