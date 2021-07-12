package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.config.TSDBConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TSDBConfigTester {
    public static void init() {
        AppInfo.setApplicationState(AppInfo.ApplicationState.TESTING);
        new Thread(() -> {
            InputStream inputStream = TSDBConfigTester.class.getResourceAsStream("/application.properties");
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
    }
}
