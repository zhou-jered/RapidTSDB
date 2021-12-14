package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.core.TSDataPoint;
import cn.rapidtsdb.tsdb.meta.BizMetric;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import cn.rapidtsdb.tsdb.server.utils.DpsUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class MultiDatapointHandler extends SimpleChannelInboundHandler<TSDataMessage.ProtoDatapoints> {

    TSDBExecutor tsdbExecutor;

    public MultiDatapointHandler() {
        this.tsdbExecutor = TSDBExecutor.getEXECUTOR();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDataMessage.ProtoDatapoints pdps) throws Exception {
        BizMetric bizMetric = DpsUtils.getBizMetric(pdps.getMetric(), pdps.getTagsList());
        TSDataPoint[] dps = DpsUtils.getDps(pdps.getDpsList());
        tsdbExecutor.write(bizMetric, dps);
    }
}
