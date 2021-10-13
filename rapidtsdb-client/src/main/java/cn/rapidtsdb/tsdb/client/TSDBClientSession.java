package cn.rapidtsdb.tsdb.client;

import cn.rapidtsdb.tsdb.model.proto.ConnectionInitia.ProtoAuthMessage;
import cn.rapidtsdb.tsdb.model.proto.ConnectionInitia.ProtoAuthResp;
import io.netty.channel.Channel;

import java.util.concurrent.atomic.AtomicReference;

import static cn.rapidtsdb.tsdb.client.ClientConnectionState.CLOSED;
import static cn.rapidtsdb.tsdb.client.ClientConnectionState.WAIT_AUTH;

public class TSDBClientSession {

    private Channel channel;
    private AtomicReference<ClientConnectionState> connectionState = new AtomicReference<>(ClientConnectionState.NONE);

    public TSDBClientSession(Channel channel) {
        this.channel = channel;
        checkoutState(WAIT_AUTH);
    }


    public ProtoAuthResp auth(ProtoAuthMessage authMessage) {
        return null;
    }

    public void close() {
        checkoutState(CLOSED);
    }

    private void checkoutState(ClientConnectionState state) {
        connectionState.set(state);
    }


    private void validChannel(Channel channel) {

    }
}
