package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.core.TSDataPoint;

import java.util.List;

public interface Aggregator {

    List<TSDataPoint> aggregator(List<TSDataPoint> dps1, List<TSDataPoint> dps2);

}
