package cn.rapidtsdb.tsdb.server.handler.rpc;

import cn.rapidtsdb.tsdb.protocol.RpcConstants;
import cn.rapidtsdb.tsdb.server.config.AppConfig;
import cn.rapidtsdb.tsdb.server.handler.rpc.common.PrimitiveObjectWriteHandler;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.V1ProcotolInitializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.util.List;

import static cn.rapidtsdb.tsdb.server.handler.rpc.ProtocolInitiaHandler.InitState;

@Log4j2
public class ProtocolInitiaHandler extends ReplayingDecoder<InitState> {


    public ProtocolInitiaHandler() {
        checkpoint(InitState.magic_number);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        InitState state = state();
        switch (state) {
            case magic_number:
                if (in.readableBytes() < 4) {
                    return;
                }
                int clientMagic = in.readInt();
                if (clientMagic == RpcConstants.MAGIC_NUMBER) {
                    checkpoint(InitState.version);
                } else {
                    checkpoint(InitState.error);
                    if (AppConfig.isDebug()) {
                        ctx.pipeline().addLast(new PrimitiveObjectWriteHandler());
                        ctx.writeAndFlush("magic number error, " + clientMagic);
                    }
                    ctx.close();
                }
                break;
            case version:
                if (in.readableBytes() < 4) {
                    return;
                }
                int clientVersion = in.readInt();
                if (clientVersion >= AppConfig.getMinimumVersion() && clientVersion <= AppConfig.getCurrentVersion()) {
                    ctx.writeAndFlush("ok");
                    ctx.writeAndFlush((short) 1);
                    launchVersion(clientVersion, ctx.pipeline());
                } else {
                    checkpoint(InitState.error);
                    ctx.writeAndFlush("UnSupprted Version");
                    ctx.close();
                }
                break;
        }
    }

    private void launchVersion(int version, ChannelPipeline pipeline) {
        switch (version) {
            case 1:
                checkoutVersion1(pipeline);
                break;
        }
    }

    private void checkoutVersion1(ChannelPipeline pipeline) {
        log.debug("Protocol checkout Version 1");
        pipeline.remove(this);
        pipeline.addLast(new V1ProcotolInitializer());
    }

    enum InitState {
        magic_number,
        version,
        error
    }
}
