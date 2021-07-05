package cn.rapidtsdb.tsdb.server.handler.console;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConsoleHandler extends SimpleChannelInboundHandler<ByteBuf> {

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        String cmd = new String(ByteBufUtil.getBytes(msg, 0, msg.readableBytes(), true));
        String[] cmdParts = cmd.trim().split("\\s+");
        String method = cmdParts[0];
        if (cmdParts.length == 1) {
            handleMethodCall(ctx, method, null);
        } else {
            String[] params = new String[cmdParts.length - 1];
            System.arraycopy(cmdParts, 1, params, 0, params.length);
            handleMethodCall(ctx, method, params);
        }
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        newLine(ctx);
    }

    private void handleMethodCall(ChannelHandlerContext ctx, String method, String... params) {
        switch (method.toLowerCase()) {
            case "bye":
                ctx.close();
                break;
            case "put":
                put(ctx, params);
                break;
            case "get":
                get(ctx, params);
                break;
            default:
                methodUnknown(ctx, method);
        }
        newLine(ctx);
    }

    private void put(ChannelHandlerContext ctx, String... params) {
        ByteBuf nb
                = ctx.alloc().buffer(10);
        nb.writeBytes("success".getBytes());
        ctx.writeAndFlush(nb);
    }

    private void get(ChannelHandlerContext ctx, String... params) {
        ByteBuf nb
                = ctx.alloc().buffer(10);
        nb.writeBytes("success".getBytes());
        ctx.writeAndFlush(nb);
    }

    private void methodUnknown(ChannelHandlerContext ctx, String method) {
        ByteBuf byteBuf = ctx.alloc().buffer(1024);
        byteBuf.writeBytes("unknown command:".getBytes());
        byteBuf.writeBytes(method.getBytes());
        ctx.writeAndFlush(byteBuf);
    }


    private void newLine(ChannelHandlerContext ctx) {
        ByteBuf newLineByteBuf = null;
        newLineByteBuf = ctx.alloc().buffer(10);
        newLineByteBuf.writeBytes("\n>>".getBytes());
        ctx.writeAndFlush(newLineByteBuf);
    }

}
