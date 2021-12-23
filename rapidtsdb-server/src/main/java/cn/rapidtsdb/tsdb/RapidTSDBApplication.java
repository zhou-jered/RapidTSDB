package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.app.AppInfo;
import cn.rapidtsdb.tsdb.app.Banner;
import cn.rapidtsdb.tsdb.config.TSDBConfig;
import cn.rapidtsdb.tsdb.core.TSDB;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.lifecycle.Runner;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
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
    private TSDBExecutor tsdbExecutor;

    public static void main(String[] args) {
        PluginManager.loadPlugins();
        Map<String, String> globalConfig = initConfig(args);
        PluginManager.configPlugins(globalConfig);
        PluginManager.preparePlugin();
        printBanner();
        RapidTSDBApplication application = new RapidTSDBApplication();
        application.init();
        application.run();
        log.info("RapidTSDB Application Start!!!");
    }


    private static Map<String, String> initConfig(String... args) {
        Map<String, String> globalConfig = new ConcurrentHashMap<>(128);

        Map<String, String> cmdProperties = CliParser.parseCmdArgs(args);
        String pn = cmdProperties.getOrDefault("config", "application.properties");
        Map<String, String> appProperties = getApplicationProperties(pn);
        Map<String, String> sysProperties = getSystemProperties();

        globalConfig.putAll(sysProperties);
        globalConfig.putAll(appProperties);
        globalConfig.putAll(cmdProperties);
        TSDBConfig.init(globalConfig);
        return globalConfig;
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
        tsdbExecutor = TSDBExecutor.getEXECUTOR();
        tsdbExecutor.init();
        loadServerInfo();
        server = new TSDBServer(serverInfo);
        server.init();
        registShutdownHook();
    }

    @Override
    public void run() {
        log.info("Application Run!!!");
        tsdbExecutor.startExecute(tsdb, TSDBConfig.getConfigInstance());
        server.run();
    }

    private void registShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("TSDB Shutting Down");
            AppInfo.setApplicationState(AppInfo.ApplicationState.SHUTDOWN);
            tsdb.close();
            tsdbExecutor.close();
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

    private static Map<String, String> getApplicationProperties(String propertiesName) {
        Map<String, String> appProperties = new ConcurrentHashMap<>(128);
        Properties properties = new Properties();
        try {
            URL appPropertiesUrl = ResourceUtils.getResourceUrl(propertiesName);
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
