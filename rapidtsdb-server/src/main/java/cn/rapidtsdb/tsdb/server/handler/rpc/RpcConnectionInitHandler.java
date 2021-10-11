package cn.rapidtsdb.tsdb.server.handler.rpc;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import static cn.rapidtsdb.tsdb.server.handler.rpc.RpcConnectionInitHandler.ConnectionInitState.version_done;

public class RpcConnectionInitHandler extends ReplayingDecoder<RpcConnectionInitHandler.ConnectionInitState> {
    public RpcConnectionInitHandler() {
        checkpoint(ConnectionInitState.init);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        switch (state()) {
            case init:
                if (in.readableBytes() >= 1) {
                    byte rpcVersion = in.readByte();
                    out.add(rpcVersion);
                    checkpoint(version_done);
                }
                break;
        }



    }

    enum ConnectionInitState {
        init,
        version_done,

    }
}
