package cn.rapidtsdb.tsdb.hbase246;

import cn.rapidtsdb.tsdb.plugins.StoreHandlerPlugin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * 插件的模式，对外提供的统一的接口，灵活化的实现，
 * 但是底层的实现会依赖到特定的配置，比如Hbase存储的配置，
 * 怎样将不同插件的配置统一起来，也是一个比较有挑战的问题，
 * 是由核心模块提供配置读取接口，插件自己去去读自己需要的配置（类似pull）
 * 还是添加一个统一化的配置接口？（类似 push ）
 * 折衷一下，于是有了下面的方案：
 * 将配置的的处理也插件化，由插件自己来处理配置文件。
 * 配置插件和存储的配合工作流程为：
 * 全局配置文件会首先处理，存储插件处理特定前缀的配置项。
 * 存储插件怎么去获取到配置插件然后读取自己需要的配置呢？
 */
public class HbaseStoreHandler implements StoreHandlerPlugin {

    private Connection connection;

    public HbaseStoreHandler() {

    }

    @Override
    public String getScheme() {
        return "hbase";
    }

    @Override
    public boolean fileExisted(String filePath) {
        return false;
    }

    @Override
    public boolean fileWriteable(String filePath) {
        return false;
    }

    @Override
    public boolean fileReadable(String filePath) {
        return false;
    }

    @Override
    public InputStream openFileInputStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public OutputStream openFileOutputStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public OutputStream openFileAppendStream(String filePath) throws IOException {
        return null;
    }

    @Override
    public File getFile(String filePath) {
        return null;
    }

    @Override
    public long getFileSize(String filePath) {
        return 0;
    }

    @Override
    public boolean createDirectory(String filePath) {
        return false;
    }

    @Override
    public boolean createFile(String filePath) {
        return false;
    }

    @Override
    public boolean removeFile(String filePath, boolean force) {
        return false;
    }
}
