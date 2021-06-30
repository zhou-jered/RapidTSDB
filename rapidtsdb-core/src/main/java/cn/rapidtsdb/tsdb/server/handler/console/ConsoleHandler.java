package cn.rapidtsdb.tsdb.server.handler.console;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class ConsoleHandler extends SimpleChannelInboundHandler<ByteBuf> {
    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        byte[] bs = new byte[msg.readableBytes()];
        msg.readBytes(bs);
        ctx.writeAndFlush(bs).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future) throws Exception {

            }

        });

        String cmd = new String(bs).trim();
        System.out.println("exec: " + cmd.trim());
        if (cmd.equals("bye")) {
            ctx.close();
        }
    }
}
