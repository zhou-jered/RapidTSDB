package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import cn.rapidtsdb.tsdb.server.utils.ProtoObjectUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
public class TSQueryHandler extends SimpleChannelInboundHandler<ProtoTSQuery> {

    private TSDBExecutor executor;

    public TSQueryHandler() {
        executor = TSDBExecutor.getEXECUTOR();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ProtoTSQuery protoTSQuery) throws Exception {

        TSQuery tsQuery = ProtoObjectUtils.getTSQuery(protoTSQuery);
        List<TSDataPoint> dps = executor.read(tsQuery);
        TSDBResponse.ProtoDataResponse dataResponse = TSDBResponse.ProtoDataResponse.newBuilder()
                .setReqId(protoTSQuery.getReqId())
                .build();

    }

}
