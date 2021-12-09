package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import cn.rapidtsdb.tsdb.server.handler.rpc.common.PrimitiveObjectWriteHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.AuthHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.ProtocolDecodeHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler.SimpleDatapointHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.out.ProtoObjectWriteHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;

public class V1ProtocolInitializer extends ChannelInitializer<NioSocketChannel> {
    @Override
    protected void initChannel(NioSocketChannel ch) {
        ch.pipeline().addLast(new ProtocolDecodeHandler(),
                new AuthHandler(),
                new SimpleDatapointHandler(),
                new ProtoObjectWriteHandler(),
                new PrimitiveObjectWriteHandler());
    }
}
