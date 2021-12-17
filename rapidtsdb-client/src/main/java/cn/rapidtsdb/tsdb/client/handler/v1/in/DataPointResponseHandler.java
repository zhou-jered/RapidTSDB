package cn.rapidtsdb.tsdb.client.handler.v1.in;

import cn.rapidtsdb.tsdb.client.utils.ChannelAttributes;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DataPointResponseHandler extends SimpleChannelInboundHandler<TSDBResponse.ProtoDataResponse> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TSDBResponse.ProtoDataResponse dataResponse) throws Exception {

        log.debug("get data:{}", dataResponse.getDpsMap());
        ChannelAttributes.getSessionAttribute(ctx)
                .setResult(dataResponse.getReqId(), dataResponse);
    }
}
