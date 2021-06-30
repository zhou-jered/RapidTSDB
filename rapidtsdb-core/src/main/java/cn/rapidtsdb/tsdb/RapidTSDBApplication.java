package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.app.Banner;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.executors.ManagedThreadPool;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import cn.rapidtsdb.tsdb.server.TSDBServer;
import cn.rapidtsdb.tsdb.utils.CliParser;
import cn.rapidtsdb.tsdb.utils.ResourceUtils;
import com.google.common.collect.Maps;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@Log4j2
public class RapidTSDBApplication implements Initializer, Runner {


    private ManagedThreadPool commonExecutor = ManagedThreadPool.getInstance();
    private TSDBServer server;
    private TSDB tsdb;

    public static void main(String[] args) {

        initConfig(args);
        printBanner();
        RapidTSDBApplication application = new RapidTSDBApplication();
        application.init();
        application.run();
        log.info("TinyTSDB Application Start!!!");
    }


    private static void initConfig(String... args) {

        Map<String, String> configKV = new ConcurrentHashMap<>(128);
        Map<String, String> appProperties = getApplicationProerties();
        Map<String, String> sysProperties = getSystemProperties();
        Map<String, String> cmdProperties = CliParser.parseCmdArgs(args);

        configKV.putAll(appProperties);
        configKV.putAll(sysProperties);
        configKV.putAll(cmdProperties);
        TSDBConfig.init(configKV);
    }

    private static void printBanner() {
        if (TSDBConfig.getConfigInstance().getPrintBanner()) {
            Banner.printBanner(System.out);
        }
    }


    @Override
    public synchronized void init() {
        log.info("start to init");
        tsdb = new TSDB();
        tsdb.init();
        server = new TSDBServer(tsdb);
        server.init();
        registShutdownHook();
    }

    public synchronized void recovery() {

    }

    @Override
    public void run() {
        log.info("Application Run!!!");
        server.run();
    }

    private void registShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("TSDB Shutting Down");
            tsdb.close();
            log.info("TSDB ShutDown Finished, Bye.");
        }));
    }

    private static Map<String, String> getApplicationProerties() {
        Map<String, String> appProperties = new ConcurrentHashMap<>(128);
        Properties properties = new Properties();
        try {
            URL appPropertiesUrl = ResourceUtils.getResourceUrl("application.properties");
            if (appPropertiesUrl != null) {
                properties.load(appPropertiesUrl.openStream());
                properties.forEach((k, v) -> {
                    appProperties.put(k.toString(), v.toString());
                });
            } else {
                log.warn("Can not get Application Properties URL");
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Load Properties Exception", e);
        }
        log.debug("load app properties: {}", properties);
        return appProperties;
    }

    private static Map<String, String> getSystemProperties() {
        Properties properties = System.getProperties();
        Map<String, String> sysMap = Maps.newHashMapWithExpectedSize(properties.size());
        properties.forEach((k, v) -> {
            sysMap.put(k.toString(), v.toString());
        });
        return sysMap;
    }

}
