package cn.rapidtsdb.tsdb.client.handler.v1;

import cn.rapidtsdb.tsdb.client.excutors.ClientRunnable;
import io.netty.channel.Channel;
import lombok.extern.log4j.Log4j2;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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

    public ClientSession regist(Channel channel) {
        final String channelId = getChannelId(channel);
        ClientSession clientSession = new ClientSession(channel);
        ClientSession oldSession = channelMap.put(channelId, clientSession);
        if (oldSession != null) {
            log.warn("regist channel multi times, {}", channelId);

        }
        return clientSession;
    }

    public boolean deregist(Channel channel) {
        final String channelId = getChannelId(channel);
        return channelMap.remove(channelId) != null;
    }


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
                boolean valid = session.checkChannelState();
                if (!valid) {
                    invalidChannelIdSet.add(channelId);
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
