package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import cn.rapidtsdb.tsdb.client.WriteMetricResult;
import cn.rapidtsdb.tsdb.client.exceptions.NoPermissionException;
import cn.rapidtsdb.tsdb.common.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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
    private Map<Integer, MsgExchange> exchangerMap = new ConcurrentHashMap<>();
    private Semaphore concurrentRequestSem;

    public ClientSession(Channel channel, TSDBClientConfig config) {
        this.channel = channel;
        final int concurrentLevel = Math.max(config.getMaxConcurrentRequestPerChannel(), 1);
        concurrentRequestSem = new Semaphore(concurrentLevel, true);
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
        checkOrWaitSessionState(ClientSessionState.PENDING_AUTH);
        return channel.pipeline().writeAndFlush(authMsg);
    }


    public WriteMetricResult write(TSDataMessage.ProtoDatapoint pdp) {
        MsgExchange<TSDataMessage.ProtoDatapoint, TSDBResponse.ProtoCommonResponse>
                msgExchange = new MsgExchange<>(pdp.getReqId(), pdp);
        return writeExchange(msgExchange);
    }

    public WriteMetricResult write(TSDataMessage.ProtoSimpleDatapoint sdp) {
        MsgExchange<TSDataMessage.ProtoSimpleDatapoint, TSDBResponse.ProtoCommonResponse>
                msgExchange = new MsgExchange<>(sdp.getReqId(), sdp);
        return writeExchange(msgExchange);
    }

    public WriteMetricResult write(TSDataMessage.ProtoDatapoints dps) {
        MsgExchange<TSDataMessage.ProtoDatapoints, TSDBResponse.ProtoCommonResponse>
                msgExchange = new MsgExchange<>(dps.getReqId(), dps);
        return writeExchange(msgExchange);
    }

    private WriteMetricResult writeExchange(MsgExchange<?, TSDBResponse.ProtoCommonResponse> msgExchange) {
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
        if (OperationPermissionMasks.hadWritePermission(permissions)) {
            exchange(msgExchange);
            TSDBResponse.ProtoCommonResponse response = msgExchange.get();
            if (response.getCode() == RpcResponseCode.SUCCESS) {
                return WriteMetricResult.OK;
            } else {
                WriteMetricResult wmr = new WriteMetricResult(false);
                wmr.setErrCode(response.getCode());
                wmr.setErrMsg(RpcResponseCode.getErrMsg(response.getCode()));
                return wmr;
            }
        } else {
            throw new NoPermissionException();
        }
    }

    public void query(TSQueryMessage.ProtoTSQuery query) {
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
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
        checkSessionState(ClientSessionState.CLOSED);
    }

    public void channeInActive() {
        log.error("channel InActive, close it.");
        close();
    }


    public void exchange(MsgExchange exchanger) {
        try {
            concurrentRequestSem.acquire();
            exchangerMap.put(exchanger.getExchangeId(), exchanger);
            Object req = exchanger.getRequest();
            channel.pipeline().writeAndFlush(req);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            concurrentRequestSem.release();
        }
    }

    public void setResult(int reqId, Object result) {
        MsgExchange msgExchange = exchangerMap.get(reqId);
        if (msgExchange != null) {
            msgExchange.setResult(result);
            exchangerMap.remove(reqId);
        } else {
            log.error("MsgExchange NULL from exchangeMap");
        }
    }


    private void checkOrWaitSessionState(ClientSessionState expectState) {
        if (sessionState() == expectState) {
            return;
        } else if (sessionState() == ClientSessionState.CLOSED) {
            throw new RuntimeException("Session Closed");
        } else {
            try {
                sessStateLock.lock();
                while (true) {
                    if (sessionState() == expectState) {
                        return;
                    } else if (sessionState() == ClientSessionState.CLOSED) {
                        throw new RuntimeException("Session Closed");
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
