package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.common.protonetty.utils.ProtoObjectUtils;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.object.TSQueryResult;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
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
        log.debug("Server query:{}", protoTSQuery.getMetrics());
        TSQuery tsQuery = ProtoObjectUtils.getTSQuery(protoTSQuery);
        TSQueryResult dps = executor.read(tsQuery);
        TSDBResponse.ProtoDataResponse dataResponse = TSDBResponse.ProtoDataResponse.newBuilder()
                .setReqId(protoTSQuery.getReqId())
                .putAllDps(dps.getDps())
                .build();
        log.debug("server send data response:{}", dps.getDps().size());
        ctx.pipeline().writeAndFlush(dataResponse);
    }

}
