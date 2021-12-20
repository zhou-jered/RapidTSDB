package cn.rapidtsdb.tsdb.hbase246;

import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Log4j2
public class HbasePreparer {

    public void checkPrepare(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(HbaseDefine.DATA_TABLE);
        boolean exist = admin.tableExists(tableName);
        if (!exist) {
            log.info("Hbase Table Not Existed, Creating");
            prepareTable(admin);
        }
    }

    private void prepareTable(Admin admin) throws IOException {
        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        for (String cf : HbaseDefine.COLUMN_FAMILIES) {
            ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder
                    .of(cf);
            descriptors.add(cfDesc);
        }
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(HbaseDefine.DATA_TABLE))
                .setColumnFamilies(descriptors)
                .build();
        admin.createTable(tableDescriptor);
    }

}
