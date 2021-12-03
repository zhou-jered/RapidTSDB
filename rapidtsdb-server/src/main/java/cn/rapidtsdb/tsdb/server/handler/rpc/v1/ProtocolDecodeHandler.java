package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.util.List;

import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.ProtocolDecodeHandler.DecodeState;
import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.ProtocolDecodeHandler.DecodeState.obj_id;

@Log4j2
public class ProtocolDecodeHandler extends ReplayingDecoder<DecodeState> {

    private short objId;
    private short objLen;
    private byte[] objBytes;

    public ProtocolDecodeHandler() {
        checkpoint(obj_id);
    }

    @Override
    protected void decode(
            ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) throws Exception {
        DecodeState state = state();
        switch (state) {
            case obj_id:
                objId = in.readShort();
                break;
            case len:
                objLen = in.readShort();
                objBytes = new byte[objLen];
                break;
            case obj:
                in.readBytes(objBytes);
        }
    }

    private Message getProtoMsg() {
        Parser<Message> msgParser = RpcObjectCode.getObjectProtoParser(objId);
        if (log.isDebugEnabled()) {
            if (msgParser == null) {
                log.error("{} get Parser null", objId);
                throw new NullPointerException("objId get Parser null");
            }
        }
        try {
            return msgParser.parseFrom(objBytes);
        } catch (InvalidProtocolBufferException e) {
            log.error("parse proto msg InvalidProtocol Exception", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    enum DecodeState {
        obj_id, //2 bytes
        len,  // 2 bytes
        obj, // obj bytes
    }
}
