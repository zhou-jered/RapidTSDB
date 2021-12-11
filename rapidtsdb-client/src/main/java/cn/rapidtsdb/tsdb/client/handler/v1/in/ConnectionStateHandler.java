package cn.rapidtsdb.tsdb.client.handler.v1.in;

import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConnectionStateHandler extends ChannelInboundHandlerAdapter {


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ClientSessionRegistry registry = ClientSessionRegistry.getRegistry();
        ClientSession clientSession = registry.getClientSession(ctx.channel());
        clientSession.close();
        registry.deregist(clientSession.getSessionId());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("channel exception", cause);
        ctx.close();
    }
}
