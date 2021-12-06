package cn.rapidtsdb.tsdb.server.handler.rpc.v1.out;

import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.GeneratedMessageV3;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class ProtoObjectWriteHandler extends ChannelOutboundHandlerAdapter {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof GeneratedMessageV3) {
            int objCode = RpcObjectCode.getObjectCode(msg.getClass());
            GeneratedMessageV3 gmv3 = (GeneratedMessageV3) msg;
            byte[] objBytes = gmv3.toByteArray();
            ctx.writeAndFlush((short) objCode);
            ctx.writeAndFlush((short) objBytes.length);
            ctx.writeAndFlush(objBytes);
        }
    }
}
