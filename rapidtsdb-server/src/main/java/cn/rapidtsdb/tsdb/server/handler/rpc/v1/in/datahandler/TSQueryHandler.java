package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.common.protonetty.utils.ProtoObjectUtils;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.object.TSQueryResult;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.exceptions.RequestIdenticalRuntimeException;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class TSQueryHandler extends SimpleChannelInboundHandler<ProtoTSQuery> {

    private TSDBExecutor executor;

    public TSQueryHandler() {
        executor = TSDBExecutor.getEXECUTOR();
    }

    /**
     * todo improve object
     *
     * @param ctx
     * @param protoTSQuery
     * @throws Exception
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ProtoTSQuery protoTSQuery) throws Exception {
        log.debug("Server query:{}, start:{}, end:{}," +
                        "agg:{}, ds:{}, tags:{}", protoTSQuery.getMetrics(), protoTSQuery.getStartTime(), protoTSQuery.getEndTime(),
                protoTSQuery.getAggregator(), protoTSQuery.getDownSampler(), protoTSQuery.getTagsMap());
        try {
            TSQuery tsQuery = ProtoObjectUtils.getTSQuery(protoTSQuery);
            TSQueryResult tsQueryResult = executor.read(tsQuery);
            TSDBResponse.ProtoDataResponse dataResponse = TSDBResponse.ProtoDataResponse.newBuilder()
                    .setReqId(protoTSQuery.getReqId())
                    .setMetric(protoTSQuery.getMetrics())
                    .putAllTags(tsQueryResult.getTags())
                    .putAllDps(tsQueryResult.getDps())
                    .setAggregator(protoTSQuery.getAggregator())
                    .setDownsampler(protoTSQuery.getDownSampler())
                    .addAllAggregatedTags(Lists.newArrayList(tsQueryResult.getAggregatedTags()))
                    .setInfo(ProtoObjectUtils.getProtoQueryStat(tsQueryResult.getInfo()))
                    .build();
            log.debug("server send data response:{}", tsQueryResult.getDps().size());
            ctx.pipeline().writeAndFlush(dataResponse);
        } catch (Exception e) {
            throw new RequestIdenticalRuntimeException(e.getMessage(), protoTSQuery.getReqId());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof RequestIdenticalRuntimeException) {
            int reqId = ((RequestIdenticalRuntimeException) cause).getReqId();
            TSDBResponse.ProtoCommonResponse exceptionalResp = TSDBResponse.ProtoCommonResponse.newBuilder()
                    .setMsg(cause.getMessage())
                    .setReqId(reqId)
                    .setException(true)
                    .setCode(RpcResponseCode.SERVER_INTERNAL_ERROR)
                    .build();
            ctx.pipeline().writeAndFlush(exceptionalResp);
        } else {
            log.error(cause);
        }
    }
}
