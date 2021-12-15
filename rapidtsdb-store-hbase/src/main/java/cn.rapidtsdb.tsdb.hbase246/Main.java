package cn.rapidtsdb.tsdb.hbase246;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Main {
    public static void main(String[] args) {
        Connection connection = getConnection();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            boolean exist = admin.tableExists(TableName.valueOf("t"));
            System.out.println("exist: " + exist);

            Table table = connection.getTable(TableName.valueOf("t"));
            Put put = new Put("h".getBytes(StandardCharsets.UTF_8));
            put.addColumn("f1".getBytes(StandardCharsets.UTF_8), "Greeting".getBytes(StandardCharsets.UTF_8),
                    "HelloJava".getBytes(StandardCharsets.UTF_8));
            table.put(put);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static Connection getConnection() {
        Configuration configuration = getConf();
        try {
            HBaseAdmin.available(configuration);
            return ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    static Configuration getConf() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "localhost");
        return configuration;
    }
}
