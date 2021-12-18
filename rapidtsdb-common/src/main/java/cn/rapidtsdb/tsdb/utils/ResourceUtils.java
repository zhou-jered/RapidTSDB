package cn.rapidtsdb.tsdb.utils;

import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

@Log4j2
public class ResourceUtils {

    public static URL getResourceUrl(String resourceName) {
        ClassLoader cl = ResourceUtils.class.getClassLoader();
        URL localResourceUrl = cl.getResource(resourceName);
        if (localResourceUrl == null) {
            log.info("FIRST get properties null");
            localResourceUrl = cl.getResource("./"+resourceName);
        }

        if (localResourceUrl == null) {
            log.info("SECOND get properties null");
            localResourceUrl = cl.getResource("classpath:application.properties");
        }

        URL systemUrl = ClassLoader.getSystemResource(resourceName);
        if(systemUrl == null) {
            File resourceFile = new File(resourceName);
            if(resourceFile.exists()) {
                log.info("using file resource: {}", resourceFile.getAbsoluteFile());
                try {
                    systemUrl = resourceFile.toURI().toURL();
                } catch (MalformedURLException e) {
                    //ignore
                }
            }
        }
        if(systemUrl!=null) {
            return systemUrl;
        }
        return localResourceUrl;
    }

}
