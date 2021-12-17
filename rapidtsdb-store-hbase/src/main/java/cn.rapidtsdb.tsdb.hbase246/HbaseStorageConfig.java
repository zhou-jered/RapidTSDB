package cn.rapidtsdb.tsdb.hbase246;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class HbaseStorageConfig {

    private String zookeeperQuorum;
    private String clientPort;

}
