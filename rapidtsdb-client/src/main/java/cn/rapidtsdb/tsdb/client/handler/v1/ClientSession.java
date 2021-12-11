package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.common.protonetty.RequestFuture;
import cn.rapidtsdb.tsdb.common.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Log4j2
public class ClientSession {

    private Channel channel;
    private AtomicReference<ClientSessionState> clientState = new AtomicReference<>(ClientSessionState.INIT);
    private Lock sessStateLock = new ReentrantLock();
    private Condition sessStateCondition = sessStateLock.newCondition();
    private int permissions = 0;
    private Map<Integer, RequestFuture> requestFutureMap = new ConcurrentHashMap<>();

    public ClientSession(Channel channel) {
        this.channel = channel;
    }


    public void versionNegotiatedCompleted() {
        checkSessionState(ClientSessionState.PENDING_AUTH);
    }

    public void authCompleted(int permissions) {
        this.permissions = permissions;
        checkSessionState(ClientSessionState.ACTIVE);
    }


    public ChannelFuture auth(
            ConnectionAuth.ProtoAuthMessage authMsg) {
        checkChannelState();
        checkOrWaitSessionState(ClientSessionState.PENDING_AUTH);
        return channel.pipeline().writeAndFlush(authMsg);
    }


    public void exchange(MsgExchange exchanger) {
        Object req = exchanger.getRequest();
        channel.pipeline().writeAndFlush(req);

    }

    public void write(TSDataMessage.ProtoDatapoint pdp) {

    }

    public RequestFuture write(TSDataMessage.ProtoSimpleDatapoint sdp) {
        checkChannelState();
//        checkOrWaitSessionState(ClientSessionState.ACTIVE);
        ChannelFuture channelFuture = channel.pipeline().writeAndFlush(sdp);
        RequestFuture requestFuture = new RequestFuture(sdp.getReqId());
        return requestFuture;
    }

    public void write(TSDataMessage.ProtoDatapoints dps) {
        checkChannelState();
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
    }

    public void query(TSQueryMessage.ProtoTSQuery query) {
        checkChannelState();
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
    }


    public void setCommonResponse(int reqId, TSDBResponse.ProtoCommonResponse commonResponse) {
        RequestFuture requestFuture = requestFutureMap.get(reqId);
        requestFuture.setResult(commonResponse);
    }

    public ChannelFuture heartbeat() {
        return null;
    }

    public String getSessionId() {
        return ChannelUtils.getChannelId(channel);
    }

    public ClientSessionState checkSessionState(ClientSessionState newState) {
        ClientSessionState origin = clientState.get();
        if (clientState.compareAndSet(origin, newState)) {
            try {
                sessStateLock.lock();
                sessStateCondition.signalAll();
            } finally {
                sessStateLock.unlock();
            }
            return newState;
        } else {
            return clientState.get();
        }
    }

    public ClientSessionState sessionState() {
        return this.clientState.get();
    }


    public void close() {
        channel.disconnect();
        channel.close();

    }

    public void channeInActive() {
        log.error("channel InActive, close it.");
        close();
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

    private void checkOrWaitSessionState(ClientSessionState expectState) {
        if (sessionState() == expectState) {
            return;
        } else {
            try {
                sessStateLock.lock();
                while (true) {
                    if (sessionState() == expectState) {
                        return;
                    }
                    sessStateCondition.await(3, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                log.warn("InterruptedException:{} ", e.getMessage());
            } finally {
                sessStateLock.unlock();
            }
        }
    }
}
