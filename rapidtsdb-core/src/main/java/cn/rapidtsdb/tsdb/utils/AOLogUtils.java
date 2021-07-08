package cn.rapidtsdb.tsdb.utils;

import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.persistent.AOLog;
import cn.rapidtsdb.tsdb.core.persistent.AppendOnlyLogManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AOLogUtils {

    public static void main(String[] args) {
        new Thread(() -> {
            InputStream inputStream = AOLogUtils.class.getResourceAsStream("/application.properties");
            Properties properties = new Properties();
            try {
                Thread.sleep(1200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                properties.load(inputStream);
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Map<String, String> ocnfig = new HashMap<>();
            properties.forEach((k, v) -> ocnfig.put(k.toString(), v.toString()));
            TSDBConfig.init(ocnfig);
        }).start();

        AppendOnlyLogManager appendOnlyLogManager = AppendOnlyLogManager.getInstance();
        appendOnlyLogManager.init();
        AOLog[] logs = appendOnlyLogManager.recoverLog(0);
        System.out.println("recover " + logs.length);
        for (int i = 0; i < logs.length; i++) {
            AOLog aoLog = logs[i];
            System.out.println(aoLog.getMetricsId() + " " + aoLog.getTimestamp() + " " + aoLog.getVal());
        }

    }

}
