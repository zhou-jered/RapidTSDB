package cn.rapidtsdb.tsdb.calculate;

import java.util.Map;

public interface Aggregator {

    Map<Long, Double> aggregate(Map<Long, Double> dps1, Map<Long, Double> dps2);

}
