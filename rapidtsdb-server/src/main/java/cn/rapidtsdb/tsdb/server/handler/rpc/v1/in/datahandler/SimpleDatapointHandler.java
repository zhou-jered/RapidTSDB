package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.common.protonetty.utils.ProtoObjectUtils;
import cn.rapidtsdb.tsdb.meta.MetricsChars;
import cn.rapidtsdb.tsdb.meta.MetricsCharsCheckResult;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse.ProtoCommonResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerClientSession;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.AttrKeys;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.SessionPermissionChangeEvent;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SimpleDatapointHandler extends SimpleChannelInboundHandler<TSDataMessage.ProtoSimpleDatapoint> {

    protected boolean authed = false;
    protected TSDBExecutor tsdbExecutor;

    public SimpleDatapointHandler() {
        tsdbExecutor = TSDBExecutor.getEXECUTOR();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDataMessage.ProtoSimpleDatapoint sdp) throws Exception {
        log.debug("get sdp:{}, {}, {} ,{}", sdp.getMetric(), sdp.getTagsMap(), sdp.getTimestamp(), sdp.getVal());
        log.debug("sdp reqId:{}", sdp.getReqId());
        if (authed) {
            BizMetric bizMetric = new BizMetric(sdp.getMetric(), sdp.getTagsMap());
            MetricsCharsCheckResult charsCheckResult = MetricsChars.checkMetricChars(bizMetric);
            if (charsCheckResult.isPass()) {
                TSDataPoint dp = ProtoObjectUtils.getDp(sdp);
                tsdbExecutor.write(bizMetric, dp);
                ProtoCommonResponse commonResponse =
                        ProtoCommonResponse.newBuilder()
                                .setReqId(sdp.getReqId())
                                .setCode(RpcResponseCode.SUCCESS).build();
                ctx.pipeline().writeAndFlush(commonResponse);
            } else {
                ProtoCommonResponse commonResponse =
                        ProtoCommonResponse.newBuilder()
                                .setReqId(sdp.getReqId())
                                .setCode(RpcResponseCode.SERVER_REFUSED)
                                .setMsg(charsCheckResult.errMsg())
                                .build();
                ctx.pipeline().writeAndFlush(commonResponse);
            }

        } else {
            ProtoCommonResponse commonResponse
                    = ProtoCommonResponse.newBuilder()
                    .setCode(RpcResponseCode.SERVER_REFUSED)
                    .setReqId(sdp.getReqId())
                    .setMsg("No Write Permission").build();
            ctx.pipeline().writeAndFlush(commonResponse);
        }
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error(cause);
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
