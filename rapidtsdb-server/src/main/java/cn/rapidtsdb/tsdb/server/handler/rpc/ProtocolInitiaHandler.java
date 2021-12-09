package cn.rapidtsdb.tsdb.server.handler.rpc;

import cn.rapidtsdb.tsdb.protocol.RpcConstants;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.server.config.AppConfig;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.V1ProtocolInitializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static cn.rapidtsdb.tsdb.server.handler.rpc.ProtocolInitiaHandler.InitState;

@Log4j2
public class ProtocolInitiaHandler extends ReplayingDecoder<InitState> {


    public ProtocolInitiaHandler() {
        checkpoint(InitState.magic_number);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.debug("new connection:{}", ctx.channel().remoteAddress());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        InitState state = state();
        switch (state) {
            case magic_number:
                int clientMagic = in.readInt();
                log.debug("read client magic:{}", Integer.toHexString(clientMagic));
                if (clientMagic == RpcConstants.MAGIC_NUMBER) {
                    checkpoint(InitState.version);
                } else {
                    checkpoint(InitState.error);
                    if (AppConfig.isDebug()) {
                        String errMsg = "unknown client, " + Integer.toHexString(clientMagic);
                        ByteBuf byteBuf = ctx.alloc().buffer(errMsg.length());
                        byteBuf.writeBytes(errMsg.getBytes(StandardCharsets.UTF_8));
                        ctx.writeAndFlush(byteBuf);
                    }
                    ctx.close();
                }
                break;
            case version:
                int clientVersion = in.readInt();
                log.debug("read client version:{}", clientVersion);
                if (clientVersion >= AppConfig.getMinimumVersion() && clientVersion <= AppConfig.getCurrentVersion()) {
                    launchVersion(clientVersion, ctx.pipeline());
                    ByteBuf versionRespBuf = ctx.alloc().buffer(8);
                    versionRespBuf.writeInt(RpcResponseCode.SUCCESS);
                    versionRespBuf.writeInt(clientVersion);
                    ChannelFuture cf = ctx.writeAndFlush(versionRespBuf);
                    cf.addListener((f) -> {
                        if (f.isSuccess()) {
                            ctx.pipeline().remove(this);
                        } else {
                            log.error("send version resp failed,", f.cause());
                        }
                    });

                } else {
                    checkpoint(InitState.error);
                    ctx.writeAndFlush("UnSupported Version");
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
        pipeline.addLast(new V1ProtocolInitializer());
    }

    enum InitState {
        magic_number,
        version,
        error
    }
}
