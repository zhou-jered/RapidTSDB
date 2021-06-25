package cn.rapidtsdb.tsdb.server.handler.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class NumberDecoderHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        log.info("writing....{} {}", msg.getClass(), msg
                .toString());
        if (msg instanceof Integer) {
            ByteBuf byteBuf = ctx.alloc().buffer();
            String writeString = "=" + String.valueOf(msg) + "\n";
            byteBuf.writeBytes(writeString.getBytes());
            ctx.writeAndFlush(byteBuf);
        } else {
            ctx.writeAndFlush(msg);
        }
    }
}
