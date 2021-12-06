package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in;

import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.plugins.ConnectionAuthPlugin;
import cn.rapidtsdb.tsdb.plugins.Permissions;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.protocol.constants.AuthTypes;
import cn.rapidtsdb.tsdb.server.defaults.DefaultAuthPlugins;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerSessionRegistry;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j2
public class AuthHandler extends SimpleChannelInboundHandler<ConnectionAuth.ProtoAuthMessage> {

    ConnectionAuthPlugin authPlugin = null;
    ServerSessionRegistry serverSessionRegistry;

    public AuthHandler() {
        this.authPlugin = PluginManager.getPlugin(ConnectionAuthPlugin.class);
        if (authPlugin == null) {
            authPlugin = new DefaultAuthPlugins();
        }
        serverSessionRegistry = ServerSessionRegistry.getRegistry();
    }

    @Override
    protected void channelRead0(
            ChannelHandlerContext channelHandlerContext,
            ConnectionAuth.ProtoAuthMessage protoAuthMessage) throws Exception {
        String authType = protoAuthMessage.getAuthType();
        log.debug("auth handle:{}", protoAuthMessage.getAuthType() + ":" + protoAuthMessage.getAuthParamsList());
        if (AuthTypes.AUTH_TYPE_TOKEN.equals(authType)) {
            List<ConnectionAuth.ProtoAuthParams> params = protoAuthMessage.getAuthParamsList();
            Map<String, String> authParams = new HashMap<>();
            if (CollectionUtils.isNotEmpty(params)) {
                params.forEach(pap -> {
                    String mayDuplicatedKeyValue = authParams.put(pap.getKey(), pap.getValue());
                    if (mayDuplicatedKeyValue != null) {
                        log.warn("duplicated key:{} in authParams, oldValue:{}, newValue:{}", pap.getKey(), mayDuplicatedKeyValue, pap.getValue());
                    }
                });
            }
            Set<Permissions> enumPermissions = authPlugin.getPermissions(protoAuthMessage.getAuthType(), protoAuthMessage.getAuthVersion(), authParams);
            ConnectionAuth.ProtoAuthResp authResp = ConnectionAuth.ProtoAuthResp.newBuilder()
                    .setMsg("ok").setAuthCode(RpcResponseCode.SUCCESS)
                    .addAllPermissions(enumPermissions.stream()
                            .map(v -> v.name()).collect(Collectors.toList())).build();
            channelHandlerContext.writeAndFlush(authResp);
            serverSessionRegistry.regist(channelHandlerContext.channel(), enumPermissions);

        } else {
            ConnectionAuth.ProtoAuthResp protoAuthResp = ConnectionAuth.ProtoAuthResp.newBuilder().setAuthCode(RpcResponseCode.AUTH_FAILED).setMsg("unsupported auth type").build();
            channelHandlerContext.writeAndFlush(protoAuthResp);
            channelHandlerContext.close();
        }
    }
}
