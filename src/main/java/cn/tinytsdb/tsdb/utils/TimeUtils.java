package cn.tinytsdb.tsdb.utils;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

public class TimeUtils {

    private static volatile long timestamp = System.currentTimeMillis();

    public static long currentTimestamp() {
        return timestamp / 1000;
    }

    public static long currentMills() {
        return timestamp;
    }


    static {
        Thread timerThread = new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                timestamp = System.currentTimeMillis();
            } catch (Exception e) {

            }
        });
        timerThread.setDaemon(true);
        timerThread.setName("tinyTSDB Timer Thread");
        timerThread.start();
    }


    public interface TimeUnitAdaptor {
        long divider = (long) 1e12;

        long adapt(long rawTimestamp);

        default int compare(long left, long right) {
            long diff = adapt(left) - adapt(right);
            return diff > 0 ? 1 : (diff < 0 ? -1 : 0);
        }
    }

    public static class TimeUnitAdaptorFactory {

        /**
         * @param timeUnit only 's' and 'ms' supported
         * @return
         */
        public static TimeUnitAdaptor getTimeAdaptor(String timeUnit) {
            Preconditions.checkNotNull(timeUnit);
            switch (timeUnit.toLowerCase().trim()) {
                case "s":
                    return ADAPTER_SECONDS;
                case "ms":
                    return ADAPTER_MILLIS_SECONDS;
                default:
                    throw new RuntimeException("Unkown time unit : " + timeUnit + ".");
            }
        }

        public static String getTimeUnitDescription() {
            return "'s' means you want an adaptor that auto transform timestamp to [second] unit\n" +
                    "'ms' means you want an adaptor that auto tranform timestamp to [million seconds] unit";
        }
    }

    private static class SecondsAdapter implements TimeUnitAdaptor {

        @Override
        public long adapt(long rawTimestamp) {
            if (rawTimestamp > divider) {
                return rawTimestamp / 1000;
            }
            return rawTimestamp;
        }
    }

    private static class MillisAdapter implements TimeUnitAdaptor {
        @Override
        public long adapt(long rawTimestamp) {
            if (rawTimestamp < divider) {
                return rawTimestamp * 1000;
            }
            return rawTimestamp;
        }
    }

    public static TimeUnitAdaptor ADAPTER_SECONDS = new SecondsAdapter();
    public static TimeUnitAdaptor ADAPTER_MILLIS_SECONDS = new MillisAdapter();

}
