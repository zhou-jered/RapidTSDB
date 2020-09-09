package cn.tinytsdb.tsdb.config;

import cn.tinytsdb.tsdb.utils.ClassUtils;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang.BooleanUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Log4j2
public class TSDBConfig {

    @Getter
    private String storeScheme = "file";
    @Getter
    private String dataDir;
    @Getter
    private String cacheDir;
    @Getter
    private String storeHandlerImplClass;

    @Getter
    private Boolean printBanner = true;

    //io executor config
    @Getter
    private Integer executorIoCore = 3;
    @Getter
    private Integer executorIoMax = 5;

    @Getter
    private AdvancedConfig advancedConfig = new AdvancedConfig();


    public static TSDBConfig getConfigInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("Config not init");
        }
        return INSTANCE;
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
        INSTANCE = new TSDBConfig();
        if (rawConfig != null) {
            for (String configItem : rawConfig.keySet()) {
                INSTANCE.setConfigVal(configItem, rawConfig.get(configItem));
            }
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
            log.warn("No [{}] config item", configItem);
        } catch (IllegalAccessException eae) {

        }
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

}
