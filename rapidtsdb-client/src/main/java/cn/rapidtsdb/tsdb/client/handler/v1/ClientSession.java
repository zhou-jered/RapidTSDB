package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import cn.rapidtsdb.tsdb.client.WriteMetricResult;
import cn.rapidtsdb.tsdb.client.exceptions.NoPermissionException;
import cn.rapidtsdb.tsdb.common.protonetty.utils.ProtoObjectUtils;
import cn.rapidtsdb.tsdb.common.utils.ChannelUtils;
import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse.ProtoCommonResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDBResponse.ProtoDataResponse;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoDatapoints;
import cn.rapidtsdb.tsdb.model.proto.TSDataMessage.ProtoSimpleDatapoint;
import cn.rapidtsdb.tsdb.model.proto.TSQueryMessage.ProtoTSQuery;
import cn.rapidtsdb.tsdb.object.TSQueryResult;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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


    public WriteMetricResult write(ProtoSimpleDatapoint sdp) {
        MsgExchange<ProtoSimpleDatapoint, ProtoCommonResponse>
                msgExchange = new MsgExchange<>(sdp.getReqId(), sdp);
        return writeExchange(msgExchange);
    }

    public WriteMetricResult write(ProtoDatapoints dps) {
        MsgExchange<ProtoDatapoints, ProtoCommonResponse>
                msgExchange = new MsgExchange<>(dps.getReqId(), dps);
        return writeExchange(msgExchange);
    }

    public TSQueryResult read(ProtoTSQuery protoQuery) {
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
        if (OperationPermissionMasks.hadReadPermission(permissions)) {
            MsgExchange<ProtoTSQuery, ProtoDataResponse> msgExchange = new MsgExchange<>(protoQuery.getReqId(), protoQuery);
            exchange(msgExchange);
            ProtoDataResponse protoDataResponse = msgExchange.get();
            TSQueryResult queryResult = TSQueryResult.builder()
                    .dps(protoDataResponse.getDpsMap())
                    .metric(protoDataResponse.getMetric())
                    .tags(protoDataResponse.getTagsMap())
                    .info(ProtoObjectUtils.getQueryState(protoDataResponse.getInfo()))
                    .aggregatedTags(protoDataResponse.getAggregatedTagsList().toArray(new String[0]))
                    .build();
            return queryResult;
        } else {
            throw new NoPermissionException("No Read Permission, permission mask:" + permissions);
        }
    }

    private WriteMetricResult writeExchange(MsgExchange<?, ProtoCommonResponse> msgExchange) {
        checkOrWaitSessionState(ClientSessionState.ACTIVE);
        if (OperationPermissionMasks.hadWritePermission(permissions)) {
            exchange(msgExchange);
            ProtoCommonResponse response = null;
            try {
                response = msgExchange.get(3, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                return new WriteMetricResult(false, -1, "timeout");
            }
            if (response.getCode() == RpcResponseCode.SUCCESS) {
                return WriteMetricResult.OK;
            } else {
                WriteMetricResult wmr = new WriteMetricResult(false);
                wmr.setErrCode(response.getCode());
                String errMsg = response.getMsg() == null ? RpcResponseCode.getErrMsg(response.getCode()) : response.getMsg();
                wmr.setErrMsg(errMsg);
                return wmr;
            }
        } else {
            throw new NoPermissionException("No Write Permission, permission mask:" + permissions);
        }
    }

    public void query(ProtoTSQuery query) {
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
        if (sessionState() == ClientSessionState.ACTIVE) {
            exchangerMap.values()
                    .forEach(val -> {
                        val.cancel();
                    });
            exchangerMap.clear();
        }
        checkSessionState(ClientSessionState.CLOSED);
        if (channel.isActive()) {
            channel.disconnect();
            channel.close();
        }
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

    public void setException(int reqId, Throwable throwable) {
        MsgExchange msgExchange = exchangerMap.get(reqId);
        if (msgExchange != null) {
            msgExchange.setException(throwable);
        } else {
            log.error("MsgExchange NULL from exchangeMap when set Exception");
        }
    }

    public void channelException(Throwable throwable) {
        exchangerMap.values().forEach(val -> {
            val.setException(throwable);
        });
        exchangerMap.clear();
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
