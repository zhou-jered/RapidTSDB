package cn.rapidtsdb.tsdb.server.handler.rpc.v1;

import cn.rapidtsdb.tsdb.plugins.Permissions;
import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ServerSessionRegistry {

    private ServerConfig serverConfig;
    private AtomicInteger idGenerator = new AtomicInteger(0);
    private Map<Integer, ServerClientSession> registry = new ConcurrentHashMap<>();

    public ServerSessionRegistry(
            ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public synchronized ServerClientSession regist(
            Channel nettyChannel,
            Set<Permissions> channelPermissions) {
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
