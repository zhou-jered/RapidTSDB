package cn.rapidtsdb.tsdb.common.protonetty.out;

import cn.rapidtsdb.tsdb.common.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessageV3;
import com.sun.xml.internal.org.jvnet.fastinfoset.FastInfosetException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ProtoObjectHandler extends ChannelOutboundHandlerAdapter {

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        log.debug("channel:{}, {} write class:{}", ChannelUtils.getChannelId(ctx.channel()), getClass().getSimpleName(), msg.getClass().getSimpleName());
        if (msg instanceof GeneratedMessageV3) {
            GeneratedMessageV3 protoMsg = (GeneratedMessageV3) msg;

            short objId = RpcObjectCode.getObjectCode(protoMsg.getClass());
            byte[] protoBytes = protoMsg.toByteArray();
            short len = (short) protoBytes.length;
            ByteBuf protoObjBuf = ctx.alloc().buffer(protoBytes.length);
            protoObjBuf.writeBytes(protoBytes);
            //optimize todo
            log.debug("proto write code:{} len:{}", objId, len);
            ctx.write(objId);
            ctx.write(len);
            ctx.write(protoObjBuf);
            ctx.flush();
            promise.setSuccess();
        }
    }

}
