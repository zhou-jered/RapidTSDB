package cn.rapidtsdb.tsdb.core;

import cn.rapidtsdb.tsdb.common.Pair;
import cn.rapidtsdb.tsdb.lifecycle.Closer;
import cn.rapidtsdb.tsdb.lifecycle.Initializer;
import cn.rapidtsdb.tsdb.plugins.FileStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.PluginManager;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Log4j2
public class QuickBlockDetector implements Initializer, Closer {

    private FileStoreHandlerPlugin fileStoreHandlerPlugin;
    private byte[][] matrix = new byte[1024][1024]; // 1024 * 1024 * 8
    private static final int X = 1024;
    private static final int Y = 1024 * 8;
    private static final String PERSIST_FILENAME = "quick.matrix";

    public boolean blockMayExist(int metricId, long baseTime) {
        Pair<Integer, Integer> xy = cal1(metricId, baseTime);
        if (!inMatrix(xy.getLeft(), xy.getRight())) {
            return false;
        }
        xy = cal2(metricId, baseTime);
        if (!inMatrix(xy.getLeft(), xy.getRight())) {
            return false;
        }
        xy = cal3(metricId, baseTime);
        if (!inMatrix(xy.getLeft(), xy.getRight())) {
            return false;
        }
        return true;
    }

    public void rememberBlock(int metricId, long basetime) {
        Pair<Integer, Integer> xy = cal1(metricId, basetime);
        setMatrix(xy.getLeft(), xy.getRight());
        xy = cal2(metricId, basetime);
        setMatrix(xy.getLeft(), xy.getRight());
        xy = cal3(metricId, basetime);
        setMatrix(xy.getLeft(), xy.getRight());

    }


    @Override
    public void close() {
        try (OutputStream outputStream = fileStoreHandlerPlugin.openFileOutputStream(PERSIST_FILENAME)) {
            for (byte[] bs : matrix) {
                outputStream.write(bs);
            }
            outputStream.flush();
        } catch (IOException e) {
            log.error(e);
        }
    }

    @Override
    public void init() {
        fileStoreHandlerPlugin = PluginManager.getPlugin(FileStoreHandlerPlugin.class);
        if (!fileStoreHandlerPlugin.fileExisted(PERSIST_FILENAME)) {
            return;
        }
        try (InputStream inputStream = fileStoreHandlerPlugin.openFileInputStream(PERSIST_FILENAME)) {
            for (int i = 0; i < X; i++) {
                byte[] row = new byte[X];
                inputStream.read(row);
                matrix[i] = row;
            }
        } catch (IOException e) {
            log.error(e);
        }
    }

    private boolean inMatrix(int x, int y) {
        byte b = matrix[x][y / X];
        return (b & BIT_PROBER[y % X % 8]) > 0;
    }

    private void setMatrix(int x, int y) {
        matrix[x][y / X] |= BIT_PROBER[y % X % 8];
    }

    private Pair<Integer, Integer> cal1(int mid, long t) {
        return Pair.of(mid % X, Math.toIntExact((t % Y)));
    }

    private Pair<Integer, Integer> cal2(int mid, long t) {
        int x = (int) (Math.abs(mid * t + 31) % X);
        int y = (int) Math.abs((mid + 37) * (t % 1000000007) % Y);
        return Pair.of(x, y);
    }


    private Pair<Integer, Integer> cal3(int mid, long t) {
        int x = (int) (Math.abs(mid * 1091 + 1039384) % X);
        int y = (int) Math.abs((mid * mid) * (t % 1000000007) % Y);
        return Pair.of(x, y);
    }


    static final byte[] BIT_PROBER = new byte[]{
            (byte) (1 << 7),
            (byte) (1 << 6),
            (byte) (1 << 5),
            (byte) (1 << 4),
            (byte) (1 << 3),
            (byte) (1 << 2),
            (byte) (1 << 1),
            (byte) (1 << 0)
    };

    public static void main(String[] args) {

    }

}
