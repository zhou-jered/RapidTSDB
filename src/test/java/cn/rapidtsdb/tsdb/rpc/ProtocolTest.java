package cn.rapidtsdb.tsdb.rpc;

import cn.rapidtsdb.tsdb.app.AppInfo;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBufUtil;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

public class ProtocolTest {

    @Test
    public void test0_rpc() {
        try {
            Socket socket = SocketChannel.open(new InetSocketAddress("localhost", 9099)).socket();
            OutputStream op = socket.getOutputStream();
            DataOutputStream dos = new DataOutputStream(op);
            dos.writeInt(AppInfo.MAGIC_NUMBER);
            System.out.println("magic bytes:" + ByteBufUtil.hexDump(Ints.toByteArray(AppInfo.MAGIC_NUMBER)));
            dos.close();
            System.out.println("done");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1_console() {
        int v = AppInfo.MAGIC_NUMBER;
        PrintStream out = System.out;
        byte[] bs = new byte[4];
        bs[0] = (byte) ((v >>> 24) & 0xFF);
        bs[1] = (byte) ((v >>> 16) & 0xFF);
        bs[2] = (byte) ((v >>> 8) & 0xFF);
        bs[3] = (byte) ((v >>> 0) & 0xFF);
        out.println(ByteBufUtil.hexDump(bs));
    }
}
