package cn.rapidtsdb.tsdb.client.handler.v1.out;

import cn.rapidtsdb.tsdb.client.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
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
        log.debug("channel:{}, {} write class:{}", ChannelUtils.getChannelId(ctx.channel()), getClass().getSimpleName(), msg.getClass().getSimpleName());
        if (msg instanceof GeneratedMessageV3) {
            GeneratedMessageV3 protoMsg = (GeneratedMessageV3) msg;

            short objId = RpcObjectCode.getObjectCode(protoMsg.getClass());
            byte[] protoBytes = protoMsg.toByteArray();
            short len = (short) protoBytes.length;
            ByteBuf nettyBuf = ctx.alloc().buffer(protoBytes.length);
            nettyBuf.writeBytes(protoBytes);
            log.debug("write proto obj:{} id:{}, len:{}, actual len:{}", protoMsg.getClass().getSimpleName(), objId, len, nettyBuf.writerIndex());
            
            ctx.writeAndFlush(objId);
            ctx.writeAndFlush(len);
            ctx.writeAndFlush(nettyBuf);
        }
    }


}
