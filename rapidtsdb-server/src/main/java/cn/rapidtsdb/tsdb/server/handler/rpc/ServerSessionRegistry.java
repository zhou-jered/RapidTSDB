package cn.rapidtsdb.tsdb.server.handler.rpc;

import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerSessionRegistry {

    private ServerConfig serverConfig;
    private AtomicInteger idGenerator = new AtomicInteger(0);
    private Map<Integer, ServerClientSession> registry = new ConcurrentHashMap<>();

    private static ServerSessionRegistry REGISTRY = null;

    public static void init(ServerConfig config) {
        if (REGISTRY != null) {
            throw new RuntimeException("Registry instance already inited");
        }
        REGISTRY = new ServerSessionRegistry(config);
    }

    public static ServerSessionRegistry getRegistry() {
        return REGISTRY;
    }

    private ServerSessionRegistry(
            ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public synchronized ServerClientSession regist(
            Channel nettyChannel,
            int channelPermissions) {
        if (registry.size() >= serverConfig.getMaxClientNumber()) {
            throw new RuntimeException("max connection client number reached");
        }
        ServerClientSession serverClientSession = new ServerClientSession(nettyChannel, channelPermissions);
        serverClientSession.setId(idGenerator.incrementAndGet());
        return serverClientSession;
    }

    public synchronized boolean deregist(
            ServerClientSession serverClientSession) {
        return registry.remove(serverClientSession.getId()) != null;
    }

    public ServerClientSession getSession(
            int sessId) {
        return registry.get(sessId);
    }


    private void capacityCheckInternal() {
        if (idGenerator.get() > Integer.MAX_VALUE / 2) {
            idGenerator.set(1024);
        }
    }
}
