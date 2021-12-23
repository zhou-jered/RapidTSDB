package cn.rapidtsdb.tsdb.server.handler.console;

import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.object.TSQueryResult;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.HashMap;
import java.util.Map;

@Log4j2
public class ConsoleHandler extends SimpleChannelInboundHandler<ByteBuf> {

    public static final String CONSOLE_RESP_NO_PARAMS = "No Parameters find.";
    public static final String CONSOLE_RESP_PARAMS_NUMBER_ERROR = "Parameters Number Error.";

    public ConsoleHandler() {

    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        String cmd = new String(ByteBufUtil.getBytes(msg, 0, msg.readableBytes(), true));
        String[] cmdParts = cmd.trim().split("\\s+");
        String method = cmdParts[0];
        if (cmdParts.length == 1) {
            handleMethodCall(ctx, method, null);
        } else {
            String[] params = new String[cmdParts.length - 1];
            System.arraycopy(cmdParts, 1, params, 0, params.length);
            handleMethodCall(ctx, method, params);
        }
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        newLine(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        writeResponse(ctx, "Exception:" + cause.getMessage());
        log.error("Exception", cause);
        newLine(ctx);
    }

    private void handleMethodCall(ChannelHandlerContext ctx, String method, String... params) {
        if (method.trim().length() == 0) {
            newLine(ctx);
            return;
        }
        switch (method.toLowerCase()) {
            case "bye":
                ctx.close();
                break;
            case "put":
                put(ctx, params);
                break;
            case "putvalue":
            case "pv":
                putValue(ctx, params);
                break;
            case "get":
                get(ctx, params);
                break;
            default:
                methodUnknown(ctx, method);
        }
    }

    /**
     * metric time value time value .....
     *
     * @param ctx
     * @param params
     */
    private void put(ChannelHandlerContext ctx, String... params) {
        if (params.length == 0) {
            writeResponse(ctx, CONSOLE_RESP_NO_PARAMS);
            return;
        }
        if (params.length == 1) {
            writeResponse(ctx, CONSOLE_RESP_PARAMS_NUMBER_ERROR + " " + "No Time and Value Parameters find.");
        }
        if (params.length < 3) {
            writeResponse(ctx, CONSOLE_RESP_PARAMS_NUMBER_ERROR);
        }
        Map<String, String> tags = null;
        for (int i = 3; i < params.length; i++) {
            String[] kv = params[i].split("=");
            if (kv.length == 2) {
                if (tags == null) {
                    tags = new HashMap<>();
                }
                tags.put(kv[0].trim(), kv[1].trim());
            }
        }
        String metric = params[0];
        long timestamp = Long.parseLong(params[1]);
        double val = Double.parseDouble(params[2]);
        boolean putResult = TSDBExecutor.getEXECUTOR().write(new BizMetric(metric, tags), new TSDataPoint(timestamp, val));
        ctx.writeAndFlush(String.valueOf(putResult));
        newLine(ctx);
    }

    private void putValue(ChannelHandlerContext ctx, String... params) {

    }

    private void get(ChannelHandlerContext ctx, String... params) {
        if (params == null || params.length < 4) {
            ctx.writeAndFlush("Params Number Error\n");
            ctx.writeAndFlush("GET ${metric} ${startTimeSeconds} ${endTimeSeconds} ${aggregator}");
        }
        String metric = params[0];
        try {
            Long startTime = Long.parseLong(params[1]);
            Long endTime = Long.parseLong(params[2]);
            if (startTime > endTime) {
                ctx.writeAndFlush("[]");
                return;
            }
            String aggregator = params[3];
            Map<String, String> tags = new HashMap<>();
            for (int i = 5; i < params.length; i++) {
                String[] kv = params[i].split("=");
                if (kv.length == 2) {
                    tags.put(kv[0], kv[1]);
                }
            }
            TSQuery tsQuery = TSQuery.builder()
                    .metric(metric)
                    .aggregator(aggregator)
                    .tags(tags)
                    .startTime(startTime)
                    .endTime(endTime)
                    .build();
            TSQueryResult qResult = null;
            if (params.length > 4) {
                tsQuery.setDownSampler(params[4]);
            }
            qResult = TSDBExecutor.getEXECUTOR().read(tsQuery);
            if (qResult == null) {
                ctx.writeAndFlush("{}");
            } else {
                String jsonStr = JSON.toJSONString(qResult, SerializerFeature.PrettyFormat,
                        SerializerFeature.WriteNonStringKeyAsString);
                ctx.writeAndFlush(jsonStr);
            }
        } catch (Exception e) {
            ctx.writeAndFlush(e.getMessage());
            ctx.writeAndFlush("\n");
        }
        newLine(ctx);
    }

    private void methodUnknown(ChannelHandlerContext ctx, String method) {
        ByteBuf byteBuf = ctx.alloc().buffer(1024);
        byteBuf.writeBytes("unknown command:".getBytes());
        byteBuf.writeBytes(method.getBytes());
        ctx.writeAndFlush(byteBuf);
        newLine(ctx);
    }


    private static void newLine(ChannelHandlerContext ctx) {
        ctx.writeAndFlush("\n>> ");
    }

    private void writeResponse(ChannelHandlerContext ctx, String resp, Object... values) {
        String line = resp;
        if (values != null && values.length > 0) {
            line = String.format(line, values);
        }
        byte[] lineBytes = line.getBytes();
        ByteBuf respBuf = ctx.alloc().buffer(lineBytes.length);
        respBuf.writeBytes(line.getBytes());
        ctx.writeAndFlush(respBuf);
    }
}
