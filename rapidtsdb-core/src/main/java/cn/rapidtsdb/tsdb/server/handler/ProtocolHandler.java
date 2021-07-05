package cn.rapidtsdb.tsdb.server.handler;

import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.server.handler.console.ConsoleHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.RpcConnectionInitHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.LineBasedFrameDecoder;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtocolHandler extends ChannelInboundHandlerAdapter {

    final int RPC_MAGIC_NUMBER = AppInfo.MAGIC_NUMBER;
    ByteBuf magicNumberBuf = null;

    public ProtocolHandler() {

    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) {
        ByteBuf msg = (ByteBuf) obj;
        while (magicNumberBuf.readableBytes() < 4 && msg.readableBytes() >= 1) {
            msg.readBytes(magicNumberBuf, 1);
        }
        if (magicNumberBuf.readableBytes() >= 4) {
            int tryMagicNumber = magicNumberBuf.readInt();
            if (tryMagicNumber == RPC_MAGIC_NUMBER) {
                
                checkoutBinaryProtocol(ctx);
            } else {
                checkoutConsoleProtocol(ctx);
                magicNumberBuf.resetReaderIndex();
                ctx.fireChannelRead(magicNumberBuf);
            }
            ctx.fireChannelRead(msg);
            ctx.pipeline().remove(this);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.magicNumberBuf = ctx.alloc().buffer(4);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("ex", cause);
        ctx.close();
    }

    private void checkoutConsoleProtocol(ChannelHandlerContext ctx) {
        ctx.pipeline().addLast(new LineBasedFrameDecoder(2048));
        ctx.pipeline().addLast(new ConsoleHandler());
    }

    private void checkoutBinaryProtocol(ChannelHandlerContext ctx) {
        ctx.pipeline().addLast(new RpcConnectionInitHandler());
    }
}
