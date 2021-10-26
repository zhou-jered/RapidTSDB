package cn.rapidtsdb.tsdb.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ClientConfigHolder {
    private static TSDBClientConfig configuration;
    private static Condition configSetCondition = new ReentrantLock().newCondition();

    public synchronized static void setConfiguration(TSDBClientConfig configuration) {
        if (ClientConfigHolder.configuration != null) {
            throw new RuntimeException("Client Config can NOT change once set");
        }
        ClientConfigHolder.configuration = configuration;
        configSetCondition.notifyAll();
    }

    public static TSDBClientConfig getConfig(long waitConfigPreparedMills) {
        if (configuration != null) {
            return configuration;
        }
        if (waitConfigPreparedMills <= 0) {
            throw new RuntimeException("Configuration not prepared");
        } else {
            while (waitConfigPreparedMills > 0 && configuration == null) {
                try {
                    configSetCondition.await(waitConfigPreparedMills, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        if (configuration == null) {
            throw new RuntimeException("Configuration Not prepared");
        }
        return configuration;
    }
}
