package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import cn.rapidtsdb.tsdb.model.proto.AuthMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class RpcClientHandler extends ChannelInboundHandlerAdapter {
    TSDBClientConfig config;

    public RpcClientHandler(TSDBClientConfig config) {
        this.config = config;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        AuthMessage.ProtoAuthMessage authMessage =  AuthMessage.ProtoAuthMessage.newBuilder()
                .build();
        authMessage.toByteArray();

    }
}
