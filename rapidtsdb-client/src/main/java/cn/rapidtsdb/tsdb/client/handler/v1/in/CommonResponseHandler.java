package cn.rapidtsdb.tsdb.client.handler.v1.in;

import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSessionRegistry;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class CommonResponseHandler extends SimpleChannelInboundHandler<TSDBResponse.ProtoCommonResponse> {

    private ClientSession clientSession;


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDBResponse.ProtoCommonResponse commonResp) throws Exception {
        if (clientSession == null) {
            clientSession = ClientSessionRegistry.getRegistry().getClientSession(ctx.channel());
        }
        log.debug("commresponse:{}, {} , {}", commonResp.getCode(), commonResp.getReqId(), commonResp.getMsg());
        int reqId = commonResp.getReqId();

        clientSession.setCommonResponse(reqId, commonResp);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("exception", cause);
    }
}
