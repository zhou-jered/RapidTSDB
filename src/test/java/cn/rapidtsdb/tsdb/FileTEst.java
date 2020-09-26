package cn.rapidtsdb.tsdb;

import org.junit.Test;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class FileTEst {

    static final int fileNum =10;
    static String getFilename(int fileIdx) {
        return "t"+fileIdx;
    }

    static void writeFile() throws IOException {

        for(int i =0;i<fileNum;i++) {
            File file = new File(getFilename(i));
            FileOutputStream fos = new FileOutputStream(file);
            System.out.println("writeing file " + i);
            for(int j =0;j<100000;j++) {
                fos.write(j>>16);
                fos.write(j);
            }
            fos.flush();
            fos.close();
        }

    }

    static void clearFile() {
        for(int i =0;i<fileNum;i++) {
            new File(getFilename(i)).deleteOnExit();
        }
    }


    static void writeTSDB() throws IOException{
        long now = System.currentTimeMillis()/1000;
        long today = now - now% TimeUnit.DAYS.toSeconds(1);
        Socket socket = new Socket("127.0.0.1",9095);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        for(int i=0;i<86400;i++) {
            String cmd = "put todaymetrics12 "+(today+i)+ " "+((int)(Math.random()*10))+" src=t\n";
            writer.write(cmd);

        }
        writer.close();
    }


    public static void main(String[] args) throws IOException {
        clearFile();
    }

    @Test
    public void testLength() {
        File file = new File("/Users/yoscript/Projects/TinyTSDB/src/test/java/com/");
        System.out.println(file.length());
    }
}
