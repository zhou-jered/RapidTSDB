package cn.tinytsdb.tsdb.calculate;

import cn.tinytsdb.tsdb.core.TSDataPoint;

import java.util.List;

public interface Aggregator {

    List<TSDataPoint> aggregator(List<TSDataPoint> dps1, List<TSDataPoint> dps2);

}
