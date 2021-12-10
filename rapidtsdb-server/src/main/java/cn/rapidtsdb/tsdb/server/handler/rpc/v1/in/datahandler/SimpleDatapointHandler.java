package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerClientSession;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.AttrKeys;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.SessionPermissionChangeEvent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SimpleDatapointHandler extends SimpleChannelInboundHandler<TSDataMessage.ProtoSimpleDatapoint> {

    private boolean authed = false;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDataMessage.ProtoSimpleDatapoint sdp) throws Exception {
        log.debug("get sdp:{}, {}, {} ,{}", sdp.getMetric(), sdp.getTagsList(), sdp.getTimestamp(), sdp.getVal());
        log.debug("sdp reqId:{}", sdp.getReqId());
        if (authed) {
            TSDBResponse.ProtoCommonResponse commonResponse =
                    TSDBResponse.ProtoCommonResponse.newBuilder()
                            .setReqId(sdp.getReqId())
                            .setCode(RpcResponseCode.SUCCESS).build();
            ctx.pipeline().writeAndFlush(commonResponse);
        } else {
            TSDBResponse.ProtoCommonResponse commonResponse
                    = TSDBResponse.ProtoCommonResponse.newBuilder()
                    .setCode(RpcResponseCode.SERVER_REFUSED)
                    .setReqId(sdp.getReqId())
                    .setMsg("No Write Permission").build();
            ctx.pipeline().writeAndFlush(commonResponse);
        }

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        super.userEventTriggered(ctx, evt);
        if (evt instanceof SessionPermissionChangeEvent) {
            refreshPermission(ctx);
        }
    }

    private void refreshPermission(ChannelHandlerContext ctx) {
        Attribute<ServerClientSession> sessAttr = ctx.channel().attr(AttributeKey.valueOf(AttrKeys.SERVER_CLIENT_SESSION));
        ServerClientSession serverClientSession = sessAttr.get();
        int permission = serverClientSession.getPermissions();
        log.debug("refresh session permission: {}", permission);
        if (OperationPermissionMasks.hadWritePermission(permission)) {
            authed = true;
        }
    }
}