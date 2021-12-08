package cn.rapidtsdb.tsdb.client.handler.v1.in;

import cn.rapidtsdb.tsdb.client.handler.v1.ProtoMsgFactory;
import com.google.protobuf.GeneratedMessageV3;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
public class ProtoMsgReaderHandler extends ReplayingDecoder<ProtoMsgReaderHandler.InBoundDecodeState> {

    short code;
    short len;


    public ProtoMsgReaderHandler() {
        checkpoint(InBoundDecodeState.obj_code);
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} registered", getClass().getSimpleName());
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} add", getClass().getSimpleName());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        InBoundDecodeState state = state();
        log.debug("proto reader state:{}", state);
        switch (state) {
            case obj_code:
                code = in.readShort();
                checkpoint(InBoundDecodeState.len);
                break;
            case len:
                len = in.readShort();
                checkpoint(InBoundDecodeState.obj_data);
                break;
            case obj_data:
                ByteBuf objBytes = in.readBytes(len);
                byte[] protoBytes = new byte[len];
                objBytes.readBytes(protoBytes);
                GeneratedMessageV3 protoMsg = ProtoMsgFactory.getProtoObject(code, protoBytes);
                out.add(protoMsg);
                checkpoint(InBoundDecodeState.obj_code);
            default:
                throw new RuntimeException("should not be here");
        }
    }

    enum InBoundDecodeState {
        obj_code,
        len,
        obj_data
    }
}
