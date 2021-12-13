package cn.rapidtsdb.tsdb.client.utils;

import cn.rapidtsdb.tsdb.client.handler.v1.AttrKeys;
import cn.rapidtsdb.tsdb.client.handler.v1.ClientSession;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public class ChannelAttributes {

    public static void setSessionAttribute(ChannelHandlerContext ctx, ClientSession sessionAttribute) {
        setSessionAttribute(ctx.channel(), sessionAttribute);
    }

    public static void setSessionAttribute(Channel channel, ClientSession sessionAttribute) {
        AttributeKey<ClientSession> attributeKey = AttributeKey.valueOf(AttrKeys.SESSION_LEY);
        Attribute<ClientSession> attribute = channel.attr(attributeKey);
        attribute.set(sessionAttribute);
    }

    public static ClientSession getSessionAttribute(ChannelHandlerContext ctx) {
        AttributeKey<ClientSession> attributeKey = AttributeKey.valueOf(AttrKeys.SESSION_LEY);
        Attribute<ClientSession> attribute = ctx.channel().attr(attributeKey);
        return attribute.get();
    }
}
