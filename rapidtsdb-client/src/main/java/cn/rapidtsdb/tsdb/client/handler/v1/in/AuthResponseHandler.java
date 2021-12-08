package cn.rapidtsdb.tsdb.client.handler.v1.in;

import cn.rapidtsdb.tsdb.client.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class AuthResponseHandler extends SimpleChannelInboundHandler<ConnectionAuth.ProtoAuthResp> {
    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("{} registered", getClass().getSimpleName());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, ConnectionAuth.ProtoAuthResp protoAuthResp) throws Exception {
        log.debug("{}, auth resp:{}", ChannelUtils.getChannelId(channelHandlerContext.channel()), protoAuthResp.toString());
        if (protoAuthResp.getAuthCode() == RpcResponseCode.SUCCESS) {
            log.info("auth success, with permission:{}", protoAuthResp.getPermissionsList());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("exception", cause);
    }
}
