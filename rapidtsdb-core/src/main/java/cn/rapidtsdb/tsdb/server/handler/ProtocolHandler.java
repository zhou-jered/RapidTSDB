package cn.rapidtsdb.tsdb.server.handler;

import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.server.handler.console.ConsoleHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.RpcConnectionInitHandler;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtocolHandler extends SimpleChannelInboundHandler<ByteBuf> {

    final int RPC_MAGIC_NUMBER = AppInfo.MAGIC_NUMBER;

    public ProtocolHandler() {

    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        if (msg.readableBytes() >= 4) {
            msg.markReaderIndex();
            int tryMagicNumber = msg.readInt();
            System.out.println("magic bytes:" + ByteBufUtil.hexDump(Ints.toByteArray(tryMagicNumber)));
            System.out.println("try int:" + tryMagicNumber + " out number:" + RPC_MAGIC_NUMBER);
            ctx.pipeline().remove(this);
            if (tryMagicNumber == RPC_MAGIC_NUMBER) {
                ctx.pipeline().addLast(new RpcConnectionInitHandler());
            } else {
                System.out.println("before set:" + msg.readableBytes());
                msg.resetReaderIndex();
                System.out.println("after set:" + msg.readableBytes());
                ctx.pipeline().addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                ctx.pipeline().addLast(new ConsoleHandler());
            }
            ByteBuf newBuf = ctx.alloc().buffer(msg.readableBytes());
            msg.readBytes(newBuf, msg.readableBytes());
            System.out.println("newBuf readable:" + newBuf.readableBytes());
            ctx.fireChannelRead(newBuf);
        } else {
            System.out.println("Waiting data");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("ex", cause);
        ctx.close();
    }
}
