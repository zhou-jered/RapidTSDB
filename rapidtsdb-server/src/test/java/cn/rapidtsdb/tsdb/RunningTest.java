package cn.rapidtsdb.tsdb;

import lombok.SneakyThrows;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

public class RunningTest {
    @SneakyThrows
    public static void main(String[] args) {
        Socket socket = SocketChannel.open(new InetSocketAddress("127.0.0.1", 9099)).socket();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream());
        final String metricName = "com.rapid.youyou";
        while (true) {
            String cmd = "put " + metricName + " " + System.currentTimeMillis() + " " + (((int) (Math.random() * 10000)) * 1.0 / 100);
            System.out.println(cmd);
            writer.write(cmd);
            writer.write("\n");
            writer.flush();
            System.out.println(reader.readLine());
            Thread.sleep(1000);
        }
    }
}
