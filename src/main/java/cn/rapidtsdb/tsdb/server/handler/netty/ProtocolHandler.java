package cn.rapidtsdb.tsdb.server.handler.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtocolHandler extends ChannelInboundHandlerAdapter {


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        ByteBuf byteBuf = (ByteBuf) msg;

        byte[] buf = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(buf);
        ReferenceCountUtil.release(msg);
        String str = new String(buf).trim();
        log.info("reading...{}", str);
        if (str.trim().equals("bye")) {
            log.info("byebye");
            ByteBuf byeBuf = ctx.alloc().buffer();
            byeBuf.writeBytes("bye\n".getBytes());
            ctx.writeAndFlush(byeBuf)
                    .addListener(f -> {
                        ctx.close();
                    });
        }
        if (str.equals("+")) {
            log.info("add adder");
            ctx.pipeline().addLast(new AddHandler());
            return;
        }
        if (str.equals("-")) {

            log.info("add suber");
            ctx.pipeline().addLast(new SubHandler());
            return;
        }
        if (str.length() > 0) {
            ctx.fireChannelRead(Integer.parseInt(str));
        }


    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("ex", cause);
    }
}
