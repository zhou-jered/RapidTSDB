package cn.rapidtsdb.tsdb;


public abstract class TsdbRunnableTask implements Runnable {
    private int retryCount = 0;

    public void markRetry() {
        retryCount++;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public abstract int getRetryLimit();

    public abstract String getTaskName();

    public static TsdbRunnableTask ofSimple(Runnable runnable) {
        return new SimpleTask(runnable);
    }

    private static class SimpleTask extends TsdbRunnableTask {
        private Runnable delegate;

        public SimpleTask(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public int getRetryLimit() {
            return 0;
        }

        @Override
        public String getTaskName() {
            return "TSDBSimpleTask";
        }

        @Override
        public void run() {
            delegate.run();
        }
    }
}
