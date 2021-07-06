package cn.rapidtsdb.tsdb.server.handler.console;

import cn.rapidtsdb.tsdb.TSDBDataOperationTask;
import cn.rapidtsdb.tsdb.TSDBTaskCallback;
import cn.rapidtsdb.tsdb.TSDataOperationQueue;
import cn.rapidtsdb.tsdb.TsdbRunnableTask;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.core.persistent.MetricsKeyManager;
import cn.rapidtsdb.tsdb.obj.WriteMetricResult;
import cn.rapidtsdb.tsdb.obj.model.Datapoint;
import cn.rapidtsdb.tsdb.server.TSDBBridge;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class ConsoleHandler extends SimpleChannelInboundHandler<ByteBuf> {

    public static final String CONSOLE_RESP_NO_PARAMS = "No Parameters find.";
    public static final String CONSOLE_RESP_PARAMS_NUMBER_ERROR = "Parameters Number Error.";

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
        newLine(ctx);
    }

    private void handleMethodCall(ChannelHandlerContext ctx, String method, String... params) {
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
        newLine(ctx);
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
                metric, new Datapoint(timestamp, val));
        operationQueue.submitTask(task);
    }

    private void putValue(ChannelHandlerContext ctx, String... params) {

    }

    private void get(ChannelHandlerContext ctx, String... params) {
        ByteBuf nb
                = ctx.alloc().buffer(10);
        nb.writeBytes("success".getBytes());
        ctx.writeAndFlush(nb);
    }

    private void methodUnknown(ChannelHandlerContext ctx, String method) {
        ByteBuf byteBuf = ctx.alloc().buffer(1024);
        byteBuf.writeBytes("unknown command:".getBytes());
        byteBuf.writeBytes(method.getBytes());
        ctx.writeAndFlush(byteBuf);
    }


    private void newLine(ChannelHandlerContext ctx) {
        ByteBuf newLineByteBuf = null;
        newLineByteBuf = ctx.alloc().buffer(10);
        newLineByteBuf.writeBytes("\n>>".getBytes());
        ctx.writeAndFlush(newLineByteBuf);
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
        private Datapoint dp;
        private int mid;

        public PutTask(TSDBTaskCallback callback, String metric, Datapoint dp) {
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
            TSDB database = TSDBBridge.getDatabase();
            WriteMetricResult writeMetricResult = null;
            try {
                writeMetricResult = database.writeMetric(metric, dp.getVal(), dp.getTimestamp());
            } catch (Exception e) {
                if (callback != null) {
                    callback.onException(this, dp, e);
                }
            }
            if (writeMetricResult.isSuccess()) {
                if (callback != null) {
                    callback.onSuccess(writeMetricResult);
                }
            } else if (callback != null) {
                callback.onFailed(this, writeMetricResult);
            }
        }
    }

    static class CommonCommandCallback implements TSDBTaskCallback {
        ChannelHandlerContext ctx;

        public CommonCommandCallback(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public Object onSuccess(Object data) {
            ByteBuf okBuf = ctx.alloc().buffer(10);
            okBuf.writeBytes("ok".getBytes());
            ctx.writeAndFlush(okBuf);
            return null;
        }

        @Override
        public void onFailed(TsdbRunnableTask task, Object data) {
            if (data instanceof WriteMetricResult) {
                WriteMetricResult wmr = (WriteMetricResult) data;
                ByteBuf byteBuf = ctx.alloc().buffer(1024);
                byteBuf.writeBytes("Failed:".getBytes());
                if (wmr.getMsg() != null) {
                    byteBuf.writeBytes(wmr.getMsg().getBytes());
                }
                ctx.writeAndFlush(byteBuf);
            }

        }

        @Override
        public void onException(TsdbRunnableTask task, Object data, Throwable exception) {
            if (data instanceof WriteMetricResult) {
                WriteMetricResult wmr = (WriteMetricResult) data;
                if (wmr.getMsg() != null) {
                    ctx.writeAndFlush("Failed:" + wmr.getMsg());
                } else {
                    ctx.writeAndFlush("Failed:code:" + wmr.getCode());
                }
            } else {
                ctx.writeAndFlush(data.toString());
            }
            if (exception != null) {
                ctx.writeAndFlush("\n");
                ctx.writeAndFlush("Exception:".getBytes());
            }
        }
    }
}
