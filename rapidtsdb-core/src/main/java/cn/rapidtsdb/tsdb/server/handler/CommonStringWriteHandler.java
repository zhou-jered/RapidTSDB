package cn.rapidtsdb.tsdb.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class CommonStringWriteHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof String) {
            byte[] bs = ((String) msg).getBytes();
            ByteBuf byteBuf = ctx.alloc().buffer(bs.length);
            byteBuf.writeBytes(bs);
            ctx.writeAndFlush(byteBuf, promise);
        } else {
            ctx.writeAndFlush(msg, promise);
        }
    }
}
