package cn.rapidtsdb.tsdb.common.protonetty.in;

import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
public class ProtocolDecodeHandler extends ReplayingDecoder<ProtocolDecodeHandler.DecodeState> {

    private short objId;
    private short objLen;
    private byte[] objBytes;

    public ProtocolDecodeHandler() {
        checkpoint(DecodeState.obj_id);
    }

    @Override
    protected void decode(
            ChannelHandlerContext ctx, ByteBuf in,
            List<Object> out) {
        DecodeState state = state();
        switch (state) {
            case obj_id:
                objId = in.readShort();
                checkpoint(DecodeState.len);
                break;
            case len:
                objLen = in.readShort();
                objBytes = new byte[objLen];
                checkpoint(DecodeState.obj);
                if (objLen > 0) {
                    // some proto object may has a 0 bytes array.
                    break;
                }
            case obj:
                in.readBytes(objBytes);
                Message protoObj = getProtoMsg();
                out.add(protoObj);
                checkpoint(DecodeState.obj_id);
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

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} channel regist: {}", getClass().getSimpleName(), ctx.channel().id().asShortText());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        log.error("exception", cause);
    }

    enum DecodeState {
        obj_id, //2 bytes
        len,  // 2 bytes
        obj, // obj bytes
    }
}
