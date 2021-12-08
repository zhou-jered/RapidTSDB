package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSessionRegistry;
import cn.rapidtsdb.tsdb.client.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.protocol.RpcConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import lombok.extern.log4j.Log4j2;

import java.util.List;

/**
 * when connection launched
 * client need to negotiate the protocol version with server,
 * the negotiate data exchange format independent with any format,
 * using the simplest way to archive it.
 *
 * <p> success situation</p>
 * [client] -------- [magic number, 4 bytes][protocol version 4 bytes]------> [server]
 * [client] <--------- [ok 2bytes] [protocol version 4 bytes] ------- [server]
 *
 * <p>failed situation</p>
 * [client] -------- [magic number, 4 bytes][protocol version 4 bytes]------> [server]
 * [client] <--------- [error msg] ------- [server]
 * ---------- XX connection closed XX ------------
 *
 * <p>
 * In case of success, the connection will checkout to specific protocol
 * data format.
 * </p>
 */
@Log4j2
public class VersionProtocolHandler extends ReplayingDecoder<VersionProtocolHandler.VersionStata> {

    private ClientSessionRegistry sessionRegistry;

    public VersionProtocolHandler() {
        sessionRegistry = ClientSessionRegistry.getRegistry();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        checkpoint(VersionStata.resp_code);
        log.info("init protocol {}", ChannelUtils.getChannelId(ctx.channel()));
        ByteBuf byteBuf = ctx.alloc().buffer(8);
        byteBuf.writeInt(RpcConstants.MAGIC_NUMBER);
        byteBuf.writeInt(RpcConstants.PROTOCOL_VERSION);
        ctx.writeAndFlush(byteBuf);
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception {
        VersionStata vs = state();
        log.debug("decoding...{}", vs);
        switch (vs) {
            case resp_code:
                int respCode = byteBuf.readInt();
                if (respCode != 0) {
                    log.error("server response failed, code:{}" + respCode);
                    channelHandlerContext.close();
                } else {
                    checkpoint(VersionStata.version);
                }
                break;
            case version:
                int version = byteBuf.readInt();
                ClientProtocolLauncher.launchProtocol(channelHandlerContext.pipeline(), version);
                channelHandlerContext.pipeline().remove(this);
                ClientSession currentSession = sessionRegistry.getClientSession(channelHandlerContext.channel());
                currentSession.versionNegotiatedCompleted();
                break;
            default:
                ;
        }

    }


    protected enum VersionStata {
        resp_code,
        version
    }

}
