package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in;

import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.ByteProcessor;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.ProtocolDecodeHandler.DecodeState;
import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.ProtocolDecodeHandler.DecodeState.len;
import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.ProtocolDecodeHandler.DecodeState.obj;
import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.ProtocolDecodeHandler.DecodeState.obj_id;

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
            List<Object> out) {
        DecodeState state = state();
        log.debug("decoding state:{}", state);
        switch (state) {
            case obj_id:
                objId = in.readShort();
                checkpoint(len);
                break;
            case len:
                objLen = in.readShort();
                objBytes = new byte[objLen];
                checkpoint(obj);
                break;
            case obj:
                AtomicInteger debugCnt = new AtomicInteger(0);
                in.forEachByte(new ByteProcessor() {
                    @Override
                    public boolean process(byte value) throws Exception {
                        debugCnt.incrementAndGet();
                        log.debug("cnt:"+debugCnt.get());
                        return true;
                    }
                });
                log.debug("try read object id:{}, len:{}, bufLen:{}", objId, objLen, debugCnt.get());
                in.readBytes(objBytes);
                Message protoObj = getProtoMsg();
                log.debug("{} add obj of class:{} to out ", getClass().getSimpleName(), protoObj.getClass().getSimpleName());
                out.add(protoObj);
                checkpoint(obj_id);
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
