package cn.rapidtsdb.tsdb.config;

import cn.rapidtsdb.tsdb.utils.ResourceUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class TestConfigLoader {

    public static void main(String[] args) {
        String fn = "application.properties";
        URL url = ResourceUtils.getResourceUrl(fn);
        System.out.println(url);
        System.out.println(url.getFile());
        try {
            System.out.println(IOUtils.toString((InputStream) url.getContent(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(url.getPath());
    }

}
