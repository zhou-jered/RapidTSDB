package cn.rapidtsdb.tsdb.client.excutors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

@Log4j2
public class ClientExecutor {
    private static final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder().setNameFormat("TSDB-Client-Scheduler-%d").build(), new AlertReject());

    public static ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        return scheduledThreadPoolExecutor;
    }

    public static class AlertReject implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (r instanceof ClientRunnable) {
                ClientRunnable cr = (ClientRunnable) r;
                if (cr.isAllowFailed()) {
                    log.warn("{} schedule task run failed", cr.getTaskName());
                } else {
                    throw new RejectedExecutionException();
                }
            }
        }
    }
}
