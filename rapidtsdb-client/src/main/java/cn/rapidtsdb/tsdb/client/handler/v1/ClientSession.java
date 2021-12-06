package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ClientSession {

    private volatile boolean valid = true;
    private Channel channel;
    private ClientSessionState clientState = ClientSessionState.INIT;

    public ClientSession(Channel channel) {
        this.channel = channel;
    }


    public ChannelFuture auth(
            ConnectionAuth.ProtoAuthMessage authMsg) {
        checkChannelState();
        return channel.writeAndFlush(authMsg);
    }

    public ChannelFuture send(Object obj) {
        checkChannelState();
        return null;
    }

    public ChannelFuture heartbeat() {
        return null;
    }

    public String getSessionId() {
        return ChannelUtils.getChannelId(channel);
    }

    public ClientSessionState checkSessionState(ClientSessionState newState) {
        this.clientState = newState;
        return this.clientState;
    }

    public ClientSessionState sessionState() {
        return this.clientState;
    }

    /**
     * called by channel registry, when channel become inactive,
     * this method will be called.
     */
    boolean checkChannelState() {
        if (!channel.isActive()) {
            throw new RuntimeException("channel inactive");
        }
        return true;
    }

    public void close() {
        channel.close();
    }
}
