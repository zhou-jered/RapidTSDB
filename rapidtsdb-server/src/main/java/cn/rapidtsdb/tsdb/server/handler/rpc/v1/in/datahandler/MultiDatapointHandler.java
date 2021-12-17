package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.meta.MetricsChars;
import cn.rapidtsdb.tsdb.meta.MetricsCharsCheckResult;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.object.BizMetric;
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
public class MultiDatapointHandler extends SimpleChannelInboundHandler<TSDataMessage.ProtoDatapoints> {


    protected boolean authed = false;
    protected TSDBExecutor tsdbExecutor;

    public MultiDatapointHandler() {
        this.tsdbExecutor = TSDBExecutor.getEXECUTOR();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDataMessage.ProtoDatapoints msdp) throws Exception {
        log.debug("get msdp:{}, {}, {} ,{}", msdp.getMetric(), msdp.getTagsMap(), msdp.getDpsCount());
        log.debug("sdp reqId:{}", msdp.getReqId());
        if (authed) {
            BizMetric bizMetric = new BizMetric(msdp.getMetric(), msdp.getTagsMap());
            MetricsCharsCheckResult charsCheckResult = MetricsChars.checkMetricChars(bizMetric);
            if (charsCheckResult.isPass()) {
                tsdbExecutor.write(bizMetric, msdp.getDpsMap());
                TSDBResponse.ProtoCommonResponse commonResponse =
                        TSDBResponse.ProtoCommonResponse.newBuilder()
                                .setReqId(msdp.getReqId())
                                .setCode(RpcResponseCode.SUCCESS).build();
                ctx.pipeline().writeAndFlush(commonResponse);
            } else {
                TSDBResponse.ProtoCommonResponse commonResponse =
                        TSDBResponse.ProtoCommonResponse.newBuilder()
                                .setReqId(msdp.getReqId())
                                .setCode(RpcResponseCode.SERVER_REFUSED)
                                .setMsg(charsCheckResult.errMsg())
                                .build();
                ctx.pipeline().writeAndFlush(commonResponse);
            }

        } else {
            TSDBResponse.ProtoCommonResponse commonResponse
                    = TSDBResponse.ProtoCommonResponse.newBuilder()
                    .setCode(RpcResponseCode.SERVER_REFUSED)
                    .setReqId(msdp.getReqId())
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
