package cn.rapidtsdb.tsdb.server.handler.console;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class ConsoleHandler extends SimpleChannelInboundHandler<ByteBuf> {

    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10, 20, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<>());

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        AtomicInteger idx = new AtomicInteger(0);
        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.submit(() -> {
                try {
                    Thread.sleep((long) (Math.random() * 1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                ByteBuf byteBuf = ctx.alloc().buffer(1024);
                byteBuf.writeBytes((Thread.currentThread().getName() + " Tick...." + idx.incrementAndGet() + "\n").getBytes());
                ctx.writeAndFlush(byteBuf);
                

            });
        }

    }
}
