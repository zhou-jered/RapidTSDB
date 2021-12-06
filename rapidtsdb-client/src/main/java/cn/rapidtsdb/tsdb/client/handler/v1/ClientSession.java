package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.exceptions.ConnectionStateException;
import cn.rapidtsdb.tsdb.client.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import io.netty.channel.Channel;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ClientSession {

    private volatile boolean valid = true;
    private Channel channel;
    private ClientConnectionState clientState = ClientConnectionState.INIT;

    public ClientSession(Channel channel) {
        this.channel = channel;

    }


    public void auth(
            ConnectionAuth.ProtoAuthMessage authMsg) {
        if (!valid) {
            throw new ConnectionStateException("channel invalid");
        }
        channel.writeAndFlush(authMsg);
    }

    public String getSessionId() {
        return ChannelUtils.getChannelId(channel);
    }

    /**
     * called by channel registry, when channel become inactive,
     * this method will be called.
     */
    boolean valid() {
        if (!channel.isActive()) {
            valid = false;
        }
        return valid;
    }

    public void close() {

    }
}
