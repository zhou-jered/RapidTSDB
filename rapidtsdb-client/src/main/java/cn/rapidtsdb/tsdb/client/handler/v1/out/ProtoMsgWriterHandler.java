package cn.rapidtsdb.tsdb.client.handler.v1.out;

import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtoMsgWriterHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GeneratedMessageV3) {
            log.debug("write protoobj:{}", msg.getClass());
            GeneratedMessageV3 protoMsg = (GeneratedMessageV3) msg;
            byte[] protoBytes = protoMsg.toByteArray();
            ByteBuf nettyBuf = ctx.alloc().buffer(protoBytes.length);
            short len = (short) protoBytes.length;
            nettyBuf.writeBytes(protoBytes);
            ctx.writeAndFlush(len);
            ctx.writeAndFlush(nettyBuf);
        }
    }
}
