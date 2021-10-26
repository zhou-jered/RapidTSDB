package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.ClientConfigHolder;
import cn.rapidtsdb.tsdb.protocol.RpcConstants;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ReplayingDecoder;

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
public class VersionProtocolHandler extends ReplayingDecoder {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        int MAGIC_NUMBER = RpcConstants.MAGIC_NUMBER;
        ByteBuf byteBuf = ctx.alloc().buffer(8);
        byteBuf.writeInt(MAGIC_NUMBER);
        byteBuf.writeInt(ClientConfigHolder.getConfig((long) 1e5)
                .getProtocolVersion());
        ctx.writeAndFlush(byteBuf);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

    }
}
