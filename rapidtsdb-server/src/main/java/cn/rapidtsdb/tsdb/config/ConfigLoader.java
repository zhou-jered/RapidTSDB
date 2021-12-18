package cn.rapidtsdb.tsdb.config;

import java.io.File;
import java.util.Properties;

public interface ConfigLoader {

    Properties loadProperties(File file);

    Properties loadYaml(File file);

    Properties loadJson(File file);

}
