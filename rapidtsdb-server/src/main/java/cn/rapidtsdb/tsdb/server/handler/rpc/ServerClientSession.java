package cn.rapidtsdb.tsdb.server.handler.rpc;

import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

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
    private int permissions;

    public ServerClientSession(Channel channel,
                               int permissions) {
        this.channel = channel;
        this.permissions = permissions;
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
        channel.disconnect();
        channel.close();
    }

    public boolean hadReadPermission() {
        return OperationPermissionMasks.hadReadPermission(permissions);
    }

    public boolean hadWritePermission() {
        return OperationPermissionMasks.hadWritePermission(permissions);
    }

    public boolean hadAdminPermission() {
        return OperationPermissionMasks.hadAdminPermission(permissions);
    }
}
