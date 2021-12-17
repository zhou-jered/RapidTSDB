package cn.rapidtsdb.tsdb.hbase246;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConnectionManager {
    private Configuration configuration = new Configuration();

    public HbaseConnectionManager(HbaseStorageConfig config) {

        configuration.set("hbase.zookeeper.property.clientPort", config.getClientPort());
        configuration.set("hbase.zookeeper.quorum", config.getZookeeperQuorum());
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
