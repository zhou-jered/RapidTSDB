package cn.rapidtsdb.tsdb.config;

import cn.rapidtsdb.tsdb.BlockCompressStrategy;
import cn.rapidtsdb.tsdb.DefaultBlockCompressStrategy;
import cn.rapidtsdb.tsdb.utils.ClassUtils;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.BooleanUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j2
public class TSDBConfig {

    /**
     * 存储在文件或者是Hbase, Hadoop, S3,
     */
    @Getter
    private String storeScheme = "file";

    /**
     * the global store path prefix
     */
    @Getter
    private String dataPath;

    /**
     * 存储接口的实现类
     */
    @Getter
    private String storeHandlerImplClass;

    @Getter
    private boolean rpcGrpcEnable = false;
    @Getter
    private String rpcGrpcIp = "0.0.0.0";
    @Getter
    private int rpcGrpcPort = 9090;

    @Getter
    private boolean rpcHttpEnable = false;
    @Getter
    private String rpcHttpIp = "0.0.0.0";
    @Getter
    private int rpcHttpPort = 9889;

    @Getter
    private Boolean printBanner = true;

    //io executor config
    @Getter
    private Integer executorIoCore = Runtime.getRuntime().availableProcessors();
    @Getter
    private Integer executorIoMax = Runtime.getRuntime().availableProcessors() * 4;

    @Getter
    Integer failedTaskExecutorThreadCoreNumber = 1;

    @Getter
    Integer failedTaskExecutorThreadMaxNumber = 5;

    @Getter
    Integer failedTaskQueueSize = 4096;

    @Getter
    Integer maxAllowedDelaySeconds = 0;

    @Getter
    Boolean allowOverwrite = false;

    @Getter
    private AdvancedConfig advancedConfig = new AdvancedConfig();

    private static Object initWaiter = new Object();

    public static TSDBConfig getConfigInstance() {
        if (INSTANCE == null) {
            synchronized (initWaiter) {
                while (true) {
                    try {
                        initWaiter.wait(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (INSTANCE != null) {
                        break;
                    }
                }
            }
        }
        return INSTANCE;
    }


    private TSDBConfig() {
    }

    @Data
    @NoArgsConstructor
    public static class AdvancedConfig {
        @Getter
        private boolean readMkIdx = true;
        @Getter
        private Integer metricsIdxCacheSize = 10240;
    }

    private static final String ADVANCED_CONFIG_PREFIX = "advance.";

    private static TSDBConfig INSTANCE = null;

    public static void init(Map<String, String> rawConfig) {
        TSDBConfig temp = new TSDBConfig();
        if (rawConfig != null) {
            for (String configItem : rawConfig.keySet()) {
                temp.setConfigVal(configItem, rawConfig.get(configItem));
            }
        }
        INSTANCE = temp;
        synchronized (initWaiter) {
            initWaiter.notifyAll();
        }
    }


    public void setConfigVal(String configItem, String val) {

        try {
            Class configClass = this.getClass();
            Object configObj = this;
            String fieldName = null;
            if (configItem.startsWith("advance.")) {
                configObj = this.advancedConfig;
                configClass = this.advancedConfig.getClass();
                fieldName = configItem2FieldName(configItem.substring(Math.min("advance.".length() + 1, configItem.length() - 1)));
            } else {
                fieldName = configItem2FieldName(configItem);
            }
            log.debug("Setting config field {} = {}", fieldName, val);
            Field configField = configClass.getDeclaredField(fieldName);
            Class fieldType = configField.getType();
            if (fieldType.equals(String.class)) {
                configField.set(configObj, val);
            } else if (fieldType.equals(Boolean.class) || fieldType.equals(Boolean.TYPE)) {
                configField.set(configObj, BooleanUtils.toBoolean(val));
            } else if (fieldType.equals(Integer.class) || fieldType.equals(Integer.TYPE)) {
                configField.set(configObj, Integer.parseInt(val));
            } else if (fieldType.equals(Long.class) || fieldType.equals(Long.TYPE)) {
                configField.set(configObj, Long.parseLong(val));
            } else if (ClassUtils.isAssignable(List.class, fieldType)) {
                String[] parts = val.split(",");
                List list = Arrays.stream(parts).map(v -> v.trim()).collect(Collectors.toList());
                configField.set(configObj, list);
            } else if (ClassUtils.isAssignable(Set.class, fieldType)) {
                String[] parts = val.split(",");
                Set set = Arrays.stream(parts).map(v -> v.trim()).collect(Collectors.toSet());
                configField.set(configObj, set);
            } else {
                throw new RuntimeException("Unsupported Config Field Class " + fieldType);
            }

        } catch (NoSuchFieldException e) {
            if (!isSystemConfig(configItem)) {
                log.warn("No [{}] config item", configItem);
            }
        } catch (IllegalAccessException eae) {

        }
    }

    public BlockCompressStrategy getBlockCompressStrategy() {
        return new DefaultBlockCompressStrategy();
    }

    private String configItem2FieldName(String item) {
        StringBuffer sb = new StringBuffer();
        boolean meetDot = false;
        for (char c : item.toCharArray()) {
            if (c == '.') {
                meetDot = true;
            } else {
                if (meetDot) {
                    sb.append(Character.toUpperCase(c));
                } else {
                    sb.append(c);
                }
                meetDot = false;
            }
        }
        return sb.toString();
    }

    private boolean isSystemConfig(String configName) {
        Properties properties = System.getProperties();
        return properties.containsKey(configName);
    }

}
