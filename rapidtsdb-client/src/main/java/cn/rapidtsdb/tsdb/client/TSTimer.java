package cn.rapidtsdb.tsdb.client;

import java.lang.reflect.Constructor;

public abstract class TSTimer {

    private static Class<? extends TSTimer> timerImplClass;

    public final static void setTimerClass(Class<? extends TSTimer> timerClass) {
        try {
            Constructor defaultNoArgsConstructor = timerClass.getConstructor();
            assert defaultNoArgsConstructor != null;
            timerClass.getConstructor();
            cachedTimer = getTimer();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static TSTimer getTimer() {
        if (timerImplClass != null) {
            try {
                return timerImplClass.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return new SystemTimer();
    }

    private static TSTimer cachedTimer = null;

    public static TSTimer getCachedTimer() {
        if (cachedTimer == null) {
            cachedTimer = getTimer();
        }
        return cachedTimer;
    }


    /**
     * return the current timestamp in million seconds units
     *
     * @return
     */
    public abstract long getCurrentMills();

    /**
     * return the current timestamp in second units
     *
     * @return
     */
    public abstract long getCurrentSecond();

    public static class SystemTimer extends TSTimer {
        @Override
        public long getCurrentMills() {
            return System.currentTimeMillis();
        }

        @Override
        public long getCurrentSecond() {
            return 0;
        }
    }
}
