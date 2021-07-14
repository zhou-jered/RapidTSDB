package cn.rapidtsdb.tsdb.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimeUtils {

    private static volatile long timestamp = System.currentTimeMillis();

    public static long currentSeconds() {
        return timestamp / 1000;
    }

    public static long currentTimestamp() {
        return timestamp;
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

    public static long getBlockBaseTimeSeconds(long timeSeconds) {
        return timeSeconds - timeSeconds % TimeUnit.HOURS.toSeconds(2);
    }

    /**
     * get the base time of the timeSeconds.
     * This code may bugly in 2038 ? Problem ?
     *
     * @param timeSeconds
     * @return
     */
    public static long truncateDaySeconds(long timeSeconds) {
        return timeSeconds - timeSeconds % TimeUnit.DAYS.toSeconds(1);
    }

    public static long truncateDayMills(long timeMills) {
        return timeMills - timeMills % TimeUnit.DAYS.toMillis(1);
    }

    public static long truncateMonthSeconds(long timeSeconds) {
        return timeSeconds - timeSeconds % TimeUnit.DAYS.toSeconds(30);
    }

    public static long truncateMonthMills(long timeMills) {
        return timeMills - timeMills % TimeUnit.DAYS.toMillis(30);
    }

    public static long truncateYearSeconds(long timeSecond) {
        return timeSecond - timeSecond % TimeUnit.DAYS.toSeconds(365);
    }

    public static long truncateYearMills(long timeMills) {
        return timeMills - timeMills % TimeUnit.DAYS.toMillis(365);
    }

    public static String formatDaily(long timestamp) {
        return dailyFormatter.get().format(new Date(timestamp));
    }

    public static String formatMonthly(long timestmp) {
        return monthlyFormatter.get().format(new Date(timestmp));
    }
    
    //formatter
    private static ThreadLocal<SimpleDateFormat> dailyFormatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));
    private static ThreadLocal<SimpleDateFormat> monthlyFormatter = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM"));

}
