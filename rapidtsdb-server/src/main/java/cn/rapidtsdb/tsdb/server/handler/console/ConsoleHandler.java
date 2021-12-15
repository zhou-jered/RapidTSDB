package cn.rapidtsdb.tsdb.server.handler.console;

import cn.rapidtsdb.tsdb.TSDBDataOperationTask;
import cn.rapidtsdb.tsdb.TSDBRunnableTask;
import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.TSDataOperationQueue;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.object.BizMetric;
import cn.rapidtsdb.tsdb.object.TSDataPoint;
import cn.rapidtsdb.tsdb.object.TSQuery;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

import java.util.List;

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

    TSDataOperationQueue operationQueue = TSDataOperationQueue.getQ();

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
        String metric = params[0];
        long timestamp = Long.parseLong(params[1]);
        double val = Double.parseDouble(params[2]);
        TSDBDataOperationTask task = new PutTask(new CommonCommandCallback(ctx),
                metric, new TSDataPoint(timestamp, val));
        operationQueue.submitTask(task);
    }

    private void putValue(ChannelHandlerContext ctx, String... params) {

    }

    private void get(ChannelHandlerContext ctx, String... params) {
        if (params == null || params.length < 3) {
            ctx.writeAndFlush("Params Number Error\n");
            ctx.writeAndFlush("GET ${metric} ${startTimeSeconds} ${endTimeSeconds}");
        }
        String metric = params[0];
        try {
            Long startTime = Long.parseLong(params[1]);
            Long endTime = Long.parseLong(params[2]);
            if (startTime > endTime) {
                ctx.writeAndFlush("[]");
                return;
            }
            TSQuery tsQuery = TSQuery.builder()
                    .metric(metric)
                    .startTime(startTime)
                    .endTime(endTime)
                    .build();
            List<TSDataPoint> dps = null;
            if (params.length > 3) {
                tsQuery.setDownSampler(params[3]);
                dps = TSDBExecutor.getEXECUTOR().read(tsQuery);
            } else {
                dps = TSDBExecutor.getEXECUTOR().read(tsQuery);
            }
            if (dps == null || dps.size() == 0) {
                ctx.writeAndFlush("[]");
            } else {
                for (int i = 0; i < dps.size(); i++) {
                    String d = dps.get(i).getTimestamp() + ":" + dps.get(i).getValue();
                    ctx.writeAndFlush(d);
                    ctx.writeAndFlush("  ");
                    if (i % 5 == 0 && i > 0) {
                        ctx.writeAndFlush("\n");
                    }
                }
            }
        } catch (NumberFormatException e) {
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

    static class PutTask extends TSDBDataOperationTask {
        private String metric;
        private TSDataPoint dp;
        private int mid;

        public PutTask(TSDBTaskCallback callback, String metric, TSDataPoint dp) {
            super(callback);
            this.metric = metric;
            this.dp = dp;
            this.mid = MetricsKeyManager.getInstance().getMetricsIndex(metric);
        }

        @Override
        public int getMetricId() {
            return mid;
        }

        @Override
        public int getRetryLimit() {
            return 0;
        }

        @Override
        public String getTaskName() {
            return null;
        }

        @Override
        public void run() {
            try {
                TSDBExecutor.getEXECUTOR().write(BizMetric.cache(metric),
                        dp);
            } catch (Exception e) {
                if (callback != null) {
                    callback.onException(this, dp, e);
                }
                callback.onFailed(this, null);
                return;
            }
            callback.onSuccess(null);
        }
    }

    static class CommonCommandCallback implements TSDBTaskCallback {
        ChannelHandlerContext ctx;

        public CommonCommandCallback(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public Object onSuccess(Object data) {
            ctx.writeAndFlush("ok");
            newLine(ctx);
            return null;
        }

        @Override
        public void onFailed(TSDBRunnableTask task, Object data) {
            ByteBuf byteBuf = ctx.alloc().buffer(15);
            byteBuf.writeBytes("Failed:".getBytes());
            ctx.writeAndFlush(byteBuf);
            newLine(ctx);
        }

        @Override
        public void onException(TSDBRunnableTask task, Object data, Throwable exception) {
            if (exception != null) {
                ctx.writeAndFlush("\n");
                ctx.writeAndFlush("Exception:" + exception.getMessage());
                log.error("", exception);
            }
            newLine(ctx);
        }
    }
}
