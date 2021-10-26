package cn.rapidtsdb.tsdb.client.handler;

import cn.rapidtsdb.tsdb.client.handler.v1.in.ProtoMsgReaderHandler;
import cn.rapidtsdb.tsdb.client.handler.v1.out.NumberWriterHandler;
import cn.rapidtsdb.tsdb.client.handler.v1.out.ProtoMsgWriterHandler;
import io.netty.channel.ChannelPipeline;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ClientProtocolLauncher {


    public static void launchProtocol(ChannelPipeline pipeline, int version) {
        switch (version) {
            case 1:
                launchVersion1(pipeline);
                break;
            default:
                ;
        }
    }

    private static void launchVersion1(ChannelPipeline pipeline) {
        log.info("Client Version Code 1");
        pipeline.addLast(new ProtoMsgReaderHandler()); // in
        pipeline.addLast(new ProtoMsgWriterHandler(),
                new NumberWriterHandler());
    }
}