package cn.rapidtsdb.tsdb.client.handler.v1.out;

import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class ProtoMsgWriter extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GeneratedMessageV3) {
            GeneratedMessageV3 protoMsg = (GeneratedMessageV3) msg;
            byte[] protoBytes = protoMsg.toByteArray();
            ByteBuf nettyBuf = ctx.alloc().buffer(protoBytes.length);
            nettyBuf.writeBytes(protoBytes);
            ctx.writeAndFlush(nettyBuf);
        }
    }
}
