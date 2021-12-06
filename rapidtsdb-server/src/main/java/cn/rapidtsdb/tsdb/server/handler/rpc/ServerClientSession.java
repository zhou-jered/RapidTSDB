package cn.rapidtsdb.tsdb.server.handler.rpc;

import cn.rapidtsdb.tsdb.plugins.Permissions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@NoArgsConstructor
@AllArgsConstructor
public class ServerClientSession {

    @Getter
    @Setter
    private int id;
    @Getter
    private long launchTime;
    @Getter
    @Setter
    private int rpcVersion;
    @Getter
    private Channel channel;
    @Getter
    private Set<Permissions> permissions = new HashSet<>();

    public ServerClientSession(Channel channel,
                               Set<Permissions> permissions) {
        this.channel = channel;
        if (permissions != null) {
            this.permissions = permissions;
        }
        launchTime = System.currentTimeMillis();
    }

    public ChannelFuture write(Object obj) {
        ChannelFuture channelFuture = channel.writeAndFlush(obj);
        return channelFuture;
    }

    public void write(Object obj,
                      GenericFutureListener listener) {
        ChannelFuture future = write(obj);
        future.addListener(listener);
    }

    public void close() {
        channel.close();
    }

    public boolean hadPermission(
            Permissions permission) {
        return permissions.contains(permission);
    }

    public boolean hadReadPermission() {
        return hadPermission(Permissions.READ);
    }

    public boolean hadWritePermission() {
        return hadPermission(Permissions.WRITE);
    }

    public boolean hadAdminPermission() {
        return hadPermission(Permissions.ADMIN);
    }
}
