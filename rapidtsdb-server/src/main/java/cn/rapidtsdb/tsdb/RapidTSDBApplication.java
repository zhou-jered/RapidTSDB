package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.app.Banner;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import cn.rapidtsdb.tsdb.server.ServerInfo;
import cn.rapidtsdb.tsdb.server.TSDBServer;
import cn.rapidtsdb.tsdb.server.config.ServerConfig;
import cn.rapidtsdb.tsdb.server.config.ServerProtocol;
import cn.rapidtsdb.tsdb.server.middleware.TSDBExecutor;
import cn.rapidtsdb.tsdb.utils.CliParser;
import cn.rapidtsdb.tsdb.utils.ResourceUtils;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import lombok.extern.log4j.Log4j2;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@Log4j2
public class RapidTSDBApplication implements Initializer, Runner {

    private TSDBServer server;
    private TSDB tsdb;
    private ServerInfo serverInfo;

    public static void main(String[] args) {
        initConfig(args);
        printBanner();
        RapidTSDBApplication application = new RapidTSDBApplication();
        application.init();
        application.run();
        log.info("RapidTSDB Application Start!!!");
    }


    private static void initConfig(String... args) {

        Map<String, String> configKV = new ConcurrentHashMap<>(128);
        Map<String, String> appProperties = getApplicationProperties();
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
        TSDBExecutor.start(tsdb);
        loadServerInfo();
        server = new TSDBServer(serverInfo);
        server.init();
        registShutdownHook();
    }

    @Override
    public void run() {
        log.info("Application Run!!!");
        server.run();
    }

    private void registShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("TSDB Shutting Down");
            AppInfo.setApplicationState(AppInfo.ApplicationState.SHUTDOWN);
            tsdb.close();
            log.info("TSDB ShutDown Finished, Bye.");
        }));
    }

    private void loadServerInfo() {
        serverInfo = new ServerInfo();
        URL serverConfigUrl = ResourceUtils.getResourceUrl("server.yaml");
        if (serverConfigUrl != null) {
            Yaml yaml = new Yaml();
            try (InputStream inputStream = serverConfigUrl.openStream();) {
                Map<String, Map> scMap = yaml.load(inputStream);
                if (scMap != null) {
                    List<ServerConfig> serverConfigList = new ArrayList<>();
                    for (String server : scMap.keySet()) {
                        Map config = scMap.get(server);
                        ServerConfig sc = JSON.parseObject(JSON.toJSONString(config), ServerConfig.class);
                        log.debug("load server config:{}", sc);
                        sc.setProtocol(ServerProtocol.valueOf(server));
                        serverConfigList.add(sc);
                    }
                    serverInfo.setConfigs(serverConfigList);
                }
            } catch (Exception e) {
                log.error("Load Server Config Exception", e);
            }
        } else {
            log.info("Using Default Server Config.");
            //todo default server config
        }
    }

    private static Map<String, String> getApplicationProperties() {
        Map<String, String> appProperties = new ConcurrentHashMap<>(128);
        Properties properties = new Properties();
        try {
            URL appPropertiesUrl = ResourceUtils.getResourceUrl("application.properties");
            if (appPropertiesUrl != null) {
                properties.load(appPropertiesUrl.openStream());
                properties.forEach((k, v) -> {
                    appProperties.put(k.toString(), v.toString());
                });
                System.out.println(appProperties);
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
