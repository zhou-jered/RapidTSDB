package cn.rapidtsdb.tsdb.hbase246;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnectionManager {
    private Configuration configuration;

    public HbaseConnectionManager(Configuration configuration) {
        this.configuration = configuration;
    }

    private Connection cachedConnection = null;
    private Object lock = new Object();

    public Connection getConnection() throws IOException {
        if (cachedConnection != null && !cachedConnection.isClosed()) {
            return cachedConnection;
        } else {
            synchronized (lock) {
                cachedConnection = ConnectionFactory.createConnection(configuration);
                return cachedConnection;
            }
        }
    }
}
