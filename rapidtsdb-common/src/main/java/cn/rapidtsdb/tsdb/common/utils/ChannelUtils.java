package cn.rapidtsdb.tsdb.common.utils;

import io.netty.channel.Channel;

public class ChannelUtils {
    public static String getChannelId(Channel channel) {
        return channel.id().asShortText();
    }
}
