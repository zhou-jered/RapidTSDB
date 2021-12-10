package cn.rapidtsdb.tsdb.server.handler.rpc.v1.in;

import cn.rapidtsdb.tsdb.model.proto.ConnectionAuth;
import cn.rapidtsdb.tsdb.plugins.ConnectionAuthPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import cn.rapidtsdb.tsdb.protocol.OperationPermissionMasks;
import cn.rapidtsdb.tsdb.protocol.RpcResponseCode;
import cn.rapidtsdb.tsdb.protocol.constants.AuthTypes;
import cn.rapidtsdb.tsdb.server.defaults.DefaultAuthPlugins;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerClientSession;
import cn.rapidtsdb.tsdb.server.handler.rpc.ServerSessionRegistry;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.AttrKeys;
import cn.rapidtsdb.tsdb.server.handler.rpc.v1.SessionPermissionChangeEvent;
import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class AuthHandler extends SimpleChannelInboundHandler<ConnectionAuth.ProtoAuthMessage> {

    ConnectionAuthPlugin authPlugin = null;

    public AuthHandler() {
        this.authPlugin = PluginManager.getPlugin(ConnectionAuthPlugin.class);
        if (authPlugin == null) {
            authPlugin = new DefaultAuthPlugins();
        }
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
            int permissions = authPlugin.getPermissions(protoAuthMessage.getAuthType(), protoAuthMessage.getAuthVersion(), authParams);
            registServerSession(channelHandlerContext.channel(), permissions);
            refreshPermission(channelHandlerContext.channel());

            ConnectionAuth.ProtoAuthResp authResp = ConnectionAuth.ProtoAuthResp.newBuilder()
                    .setMsg("ok").setAuthCode(RpcResponseCode.SUCCESS)
                    .setPermissions(OperationPermissionMasks.RW_PERMISSION)
                    .setReqId(protoAuthMessage.getReqId())
                    .build();
            channelHandlerContext.pipeline().writeAndFlush(authResp);

        } else {
            ConnectionAuth.ProtoAuthResp protoAuthResp = ConnectionAuth.ProtoAuthResp.newBuilder().setAuthCode(RpcResponseCode.AUTH_FAILED).setMsg("unsupported auth type")
                    .setReqId(protoAuthMessage.getReqId())
                    .build();
            channelHandlerContext.writeAndFlush(protoAuthResp);
            channelHandlerContext.disconnect();
            channelHandlerContext.close();
        }
    }


    private void registServerSession(Channel channel, int permissions) {
        ServerClientSession scs = ServerSessionRegistry.getRegistry().regist(channel, permissions);
        AttributeKey attrKey = AttributeKey.valueOf(AttrKeys.SERVER_CLIENT_SESSION);
        Attribute<ServerClientSession> attribute = channel.attr(attrKey);
        attribute.set(scs);
    }

    private void refreshPermission(Channel channel) {
        channel.pipeline().fireUserEventTriggered(new SessionPermissionChangeEvent());
    }
}
