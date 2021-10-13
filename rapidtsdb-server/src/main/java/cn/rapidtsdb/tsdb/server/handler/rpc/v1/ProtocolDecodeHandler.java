package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.ProtocolDecodeHandler.DecodeState;
import static cn.rapidtsdb.tsdb.server.handler.rpc.v1.ProtocolDecodeHandler.DecodeState.obj_id;

public class ProtocolDecodeHandler extends ReplayingDecoder<DecodeState> {
    public ProtocolDecodeHandler() {
        checkpoint(obj_id);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        DecodeState state = state();
        switch (state) {
            case obj_id:
        }
    }

    enum DecodeState {
        obj_id, //2 bytes
        len,  // 2 bytes
        obj, // obj bytes
    }
}
