package cn.rapidtsdb.tsdb.server.handler.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;

@Log4j2
public class SubHandler extends ChannelInboundHandlerAdapter {
    List<Integer> nums = new ArrayList<>();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        nums.add((Integer) msg);
        if (nums.size() > 1) {
            int result = nums.get(0) - nums.get(1);
            nums.remove(0);
            nums.remove(0);
            ctx.writeAndFlush(result);
            ctx.pipeline().remove(this);
        }
    }

}
