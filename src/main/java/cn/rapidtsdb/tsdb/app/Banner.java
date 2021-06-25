package cn.rapidtsdb.tsdb.app;

import cn.rapidtsdb.tsdb.utils.ResourceUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;


public class Banner {

    public static void printBanner(PrintStream outputStream) {
        URL bannerUrl = ResourceUtils.getResourceUrl("banner.txt");
        if (bannerUrl == null) {
            return;
        }
        InputStream inputStream = null;
        try {
            inputStream = bannerUrl.openStream();
        } catch (IOException e) {
            return;
        }
        StringBuffer sb = new StringBuffer();
        char[] buffer = new char[4096];
        if (inputStream != null) {
            InputStreamReader isr = new InputStreamReader(inputStream);
            try {
                while (true) {
                    int readBytes = isr.read(buffer);
                    if (readBytes > 0) {
                        sb.append(buffer, 0, readBytes);
                    } else {
                        break;
                    }
                }
                outputStream.println(sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
