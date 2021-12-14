package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import cn.rapidtsdb.tsdb.common.protonetty.in.ProtocolDecodeHandler;
import cn.rapidtsdb.tsdb.common.protonetty.out.PrimitiveObjectWriteHandler;
import cn.rapidtsdb.tsdb.common.protonetty.out.ProtoObjectHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.AuthHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler.MultiDatapointHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler.SimpleDatapointHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.nio.NioSocketChannel;

public class V1ProtocolInitializer extends ChannelInitializer<NioSocketChannel> {

    public V1ProtocolInitializer() {
        super();
    }

    @Override
    protected void initChannel(NioSocketChannel ch) {
        ch.pipeline().addLast(new ProtocolDecodeHandler(),
                new AuthHandler(),
                new SimpleDatapointHandler(),
                new MultiDatapointHandler(),
                new PrimitiveObjectWriteHandler(),
                new ProtoObjectHandler());
    }
}
