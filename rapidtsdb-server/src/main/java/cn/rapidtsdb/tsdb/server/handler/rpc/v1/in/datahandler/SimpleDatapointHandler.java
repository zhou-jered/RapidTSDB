package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in.datahandler;

import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SimpleDatapointHandler extends SimpleChannelInboundHandler<TSDataMessage.ProtoSimpleDatapoint> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDataMessage.ProtoSimpleDatapoint msg) throws Exception {
        log.debug("get sdp:{}, {}, {} ,{}", msg.getMetric(), msg.getTagsList(), msg.getTimestamp(), msg.getVal());
        TSDBResponse.ProtoCommonResponse commonResponse =
                TSDBResponse.ProtoCommonResponse.newBuilder()
                        .setCode(RpcResponseCode.SUCCESS).build();
        ctx.writeAndFlush(commonResponse);


    }
}
