package cn.rapidtsdb.tsdb.hbase246;

import cn.rapidtsdb.tsdb.plugins.BlockStoreHandlerPlugin;
import cn.rapidtsdb.tsdb.plugins.pojo.BlockPojo;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Log4j2
public class HbaseBlockStoreHandler implements BlockStoreHandlerPlugin {

    private HbaseConnectionManager connectionManager;
    private Table dataTable;

    @Override
    public boolean blockExists(int metricid, long basetime) {
        return true;
    }

    @Override
    public void storeBlock(int metricId, long baseTime, byte[] data) {
        Put put = new Put(Ints.toByteArray(metricId));
        put.addColumn(HbaseDefine.CF_DATA_BYTES, Longs.toByteArray(baseTime), data);
        try {
            dataTable.put(put);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getBlockData(int metricId, long baseTime) {
        Get get = new Get(Ints.toByteArray(metricId));
        byte[] cf = HbaseDefine.CF_DATA_BYTES;
        byte[] qualifier = Longs.toByteArray(baseTime);
        get.addColumn(cf, qualifier);
        try {
            Result result = dataTable.get(get);
            byte[] data = result.getValue(cf, qualifier);
            return data;
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public BlockPojo[] multiGetBlock(int metricId, Iterator<Long> basetimeIter) {
        byte[] row = Ints.toByteArray(metricId);
        byte[] cf = HbaseDefine.CF_DATA_BYTES;
        Get get = new Get(row);
        while (basetimeIter.hasNext()) {
            long t = basetimeIter.next();
            get.addColumn(cf, Longs.toByteArray(t));
        }
        try {
            List<BlockPojo> bpList = new LinkedList<>();
            Result result = dataTable.get(get);
            CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()) {
                Cell cell = cellScanner.current();
                byte[] baseTimeBytes = cell.getQualifierArray();
                byte[] value = cell.getValueArray();
                BlockPojo bp = new BlockPojo(metricId, Longs.fromByteArray(baseTimeBytes), value);
                bpList.add(bp);
            }
            BlockPojo[] bpArray = bpList.toArray(new BlockPojo[0]);
            return bpArray;
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public BlockPojo[] multiGetBlock(int[] metricIds, long basetime) {
        return new BlockPojo[0];
    }

    @Override
    public Map<Integer, BlockPojo[]> multiGetBlock(int[] metricIds, Iterator<Long> basetimeScanner) {
        return null;
    }

    @Override
    public Stream<BlockPojo> scanBlocks(int metricId, Iterator<Long> basetimeIter) {
        return null;
    }

    @Override
    public Stream<Map<Integer, BlockPojo>> crossScanBlocks(int[] metricId, Iterator<Long> basetimeScanner) {
        return null;
    }

    @Override
    public String getInterestedPrefix() {
        return "store.hbase";
    }

    @Override
    public void config(Map<String, String> subConfig) {
        Configuration configuration = new Configuration();
        subConfig.forEach((k, v) -> {
            String hbaseConfig = k.substring(6);
            configuration.set(hbaseConfig, v);
            log.debug("{} config {} -> {}", getClass().getSimpleName(), hbaseConfig, v);
        });
        connectionManager = new HbaseConnectionManager(configuration);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void prepare() {
        try {
            dataTable = connectionManager.getConnection().getTable(TableName.valueOf(HbaseDefine.DATA_TABLE));
            HbasePreparer tablePreparer = new HbasePreparer();
            tablePreparer.checkPrepare(connectionManager.getConnection());
            log.info("{} prepare done", getClass().getSimpleName());
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }
}
