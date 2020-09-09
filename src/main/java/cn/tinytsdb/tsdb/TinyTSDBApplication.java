package cn.tinytsdb.tsdb;

import cn.tinytsdb.tsdb.app.Banner;
import cn.tinytsdb.tsdb.config.TSDBConfig;
import cn.tinytsdb.tsdb.context.AppContext;
import cn.tinytsdb.tsdb.core.TSDB;
import cn.tinytsdb.tsdb.exectors.GlobalExecutorHolder;
import cn.tinytsdb.tsdb.lifecycle.Initializer;
import cn.tinytsdb.tsdb.lifecycle.Runner;
import cn.tinytsdb.tsdb.rpc.RpcManager;
import cn.tinytsdb.tsdb.utils.CliParser;
import cn.tinytsdb.tsdb.utils.ResourceUtils;
import com.alibaba.fastjson.JSON;
import lombok.Setter;
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
public class TinyTSDBApplication implements Initializer, Runner {

    private AppContext appContext;

    @Setter
    private String env = AppContext.DEFAULT_ENV;

    private GlobalExecutorHolder commonExecutor = GlobalExecutorHolder.getInstance();
    private RpcManager rpcManager;
    private TSDB tsdb;


    public static void main(String[] args) {
        log.debug("debug");
        Map<String, String> configKV = new ConcurrentHashMap<>(128);
        Map<String, String> appProperties = getApplicationProerties();
        Map<String, String> cmdProperties = CliParser.parseCmdArgs(args);
        configKV.putAll(appProperties);
        configKV.putAll(cmdProperties);

        String env = configKV.getOrDefault("env", AppContext.DEFAULT_ENV);
        TSDBConfig.init(configKV);

        log.debug("tsdbconfig :{}", JSON.toJSONString(TSDBConfig.getConfigInstance()));
        if (TSDBConfig.getConfigInstance().getPrintBanner()) {
            Banner.printBanner(System.out);
        }
        TinyTSDBApplication application = new TinyTSDBApplication();
        application.setEnv(env);
        application.init();
        application.run();
        log.info("TinyTSDB Application Start!!!");
    }

    @Override
    public void init() {

        appContext = AppContext.getDefaultContext();
        registShutdownHook();
    }

    @Override
    public void run() {
        log.info("Application Run!!!");
    }

    private void registShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("TSDB Shutting Down");
            if (appContext != null) {
                appContext.close();
            }
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


}
