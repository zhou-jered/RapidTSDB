package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.TSDBClientConfig;
import cn.rapidtsdb.tsdb.client.excutors.ClientRunnable;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import lombok.extern.log4j.Log4j2;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static cn.rapidtsdb.tsdb.common.utils.ChannelUtils.getChannelId;

@Log4j2
public class ClientSessionRegistry {

    private static final ClientSessionRegistry REGISTRY = new ClientSessionRegistry();

    public static ClientSessionRegistry getRegistry() {
        return REGISTRY;
    }

    public ClientSessionRegistry() {
    }

    private Map<String, ClientSession> channelMap = new ConcurrentHashMap<>();


    public ClientSession getClientSession(Channel channel) {
        return channelMap.get(getChannelId(channel));
    }

    public ClientSession regist(Channel channel, TSDBClientConfig config) {
        final String channelId = getChannelId(channel);
        ClientSession clientSession = new ClientSession(channel, config);
        ClientSession oldSession = channelMap.put(channelId, clientSession);
        if (oldSession != null) {
            log.warn("regist channel multi times, {}", channelId);

        }
        return clientSession;
    }

    public boolean deregist(String sessId) {
        return channelMap.remove(sessId) != null;
    }

    public boolean deregist(Channel channel) {
        final String sessId = getChannelId(channel);
        return deregist(sessId);
    }

    public void scheduleRegistry(EventLoopGroup eventExecutors) {
        eventExecutors.scheduleAtFixedRate(new CheckExpiredSessionTask(),
                5, 5, TimeUnit.SECONDS
        );
    }

    //todo improved need
    static class CheckExpiredSessionTask extends ClientRunnable {
        public CheckExpiredSessionTask() {
            super("CheckExpiresSessionTask", true);
        }

        @Override
        public void run() {
            Map<String, ClientSession> channelMap = REGISTRY.channelMap;
            Set<String> invalidChannelIdSet = new HashSet<>();

            for (String channelId : channelMap.keySet()) {
                ClientSession session = channelMap.get(channelId);
                if (session.sessionState() == ClientSessionState.CLOSED) {
                    invalidChannelIdSet.add(session.getSessionId());
                }
            }
            if (!invalidChannelIdSet.isEmpty()) {
                invalidChannelIdSet.forEach(cid -> {
                    channelMap.remove(cid);
                });
            }
        }
    }


}
