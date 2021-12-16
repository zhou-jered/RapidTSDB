package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.common.protonetty.utils.ProtoObjectUtils;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

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
        List<TSDataPoint> dps = executor.read(tsQuery);
        List<TSDataMessage.ProtoDatapoint> protoDps = new ArrayList<>(dps.size());
        if (dps.size() > 0) {
            dps.forEach(dp -> {
                protoDps.add(TSDataMessage.ProtoDatapoint.newBuilder().setTimestamp(dp.getTimestamp())
                        .setVal(dp.getValue()).build());
            });
        }
        TSDataMessage.ProtoDatapoints pdps = TSDataMessage.ProtoDatapoints.newBuilder()
                .addAllDps(protoDps)
                .build();
        TSDBResponse.ProtoDataResponse dataResponse = TSDBResponse.ProtoDataResponse.newBuilder()
                .setReqId(protoTSQuery.getReqId())
                .setDps(pdps)
                .build();
        log.debug("server send data response:{}", pdps.getDpsCount());
        ctx.pipeline().writeAndFlush(dataResponse);
    }

}
