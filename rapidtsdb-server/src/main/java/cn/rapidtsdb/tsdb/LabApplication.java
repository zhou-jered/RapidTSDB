package cn.rapidtsdb.tsdb;

import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class LabApplication {
    public static void main(String[] args) throws Exception{
        System.out.println("Lab Application Running with..."+ Arrays.toString(args));
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        Enumeration<URL> classUrlIter = null;
        String packageName = "cn/rapidtsdb/tsdb";
        if(args.length>0) {
            packageName=args[0];
            System.out.println("package:"+packageName);
        }

        List<String> allClassPath = new ArrayList<>();
        Queue<String> Q = new LinkedBlockingQueue<>();
        Q.add(packageName);

        while(Q.isEmpty()==false) {
            String roundPath = Q.poll();

            classUrlIter = classloader.getResources(roundPath);
            while(classUrlIter.hasMoreElements()) {
                URL url = classUrlIter.nextElement();
                InputStream inputStream = url.openStream();
                String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                if(StringUtils.isEmpty(content.trim())==false) {
                    String[] subs = content.split("\n");
                    for(String sub : subs) {
                        String subPath = roundPath+"/"+sub;
                        if(sub.endsWith(".class")) {
                            allClassPath.add(subPath);
                        } else {
                            File file = new File(url.getFile()+"/"+sub);
                            if(file.isDirectory()) {
                                Q.add(subPath);
                            } else {
                                System.out.println("exclude file:"+subPath);
                            }
                        }
                    }
                }

            }
        }
        System.out.println("Total: "+allClassPath.size());
        for(String s : allClassPath) {
            String className = s.replace("/",".");
            className = className.substring(0, className.length()-6);
            System.out.println(className);
            Class cls = Class.forName(className);
            if(cls.isAssignableFrom(Initializer.class)) {
                System.out.println(className);
            }
        }
        System.out.println("Done");

    }

    static void exploreClass(URL parentUrl) throws Exception{
        URLConnection urlConnection = parentUrl.openConnection();
        System.out.println(IOUtils.toString(urlConnection.getInputStream()));
        urlConnection.getInputStream().close();
    }


}
