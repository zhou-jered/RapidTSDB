package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.client.exceptions.ConnectionException;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.protocol.RpcObjectCode;
import io.netty.channel.Channel;

import java.util.concurrent.atomic.AtomicReference;

import static cn.rapidtsdb.tsdb.client.ClientConnectionState.CLOSED;
import static cn.rapidtsdb.tsdb.client.ClientConnectionState.WAIT_AUTH;

public class TSDBClientSession {

    private Channel channel;
    private AtomicReference<ClientConnectionState> connectionState = new AtomicReference<>(ClientConnectionState.NONE);
    private long start;

    public TSDBClientSession(Channel channel) {
        this.channel = channel;
        this.start = System.currentTimeMillis();
        checkoutState(WAIT_AUTH);
    }

    public ConnectionAuth.ProtoAuthResp auth(ConnectionAuth.ProtoAuthMessage authMessage) {
        validChannel(channel);
        short code = RpcObjectCode.getObjectCode(ConnectionAuth.ProtoAuthMessage.class.getCanonicalName());
        channel.writeAndFlush(code);
        channel.writeAndFlush(authMessage);
        return null;
    }

    
    public void close() {
        checkoutState(CLOSED);
    }

    private void checkoutState(ClientConnectionState state) {
        connectionState.set(state);
    }

    private void validChannel(Channel channel) {
        if (!channel.isActive()) {
            throw new ConnectionException("Connection InActive");
        }
    }
}
