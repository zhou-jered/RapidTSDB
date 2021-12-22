package cn.rapidtsdb.tsdb.cal;

import cn.rapidtsdb.tsdb.calculate.Aggregator;
import cn.rapidtsdb.tsdb.calculate.CalculatorFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AggregatorTest {
    @Test
    public void test1() {
        Aggregator aggregator = CalculatorFactory.getAggregator("sum");
        Map<Long, Double> dps1 = new HashMap<>();
        Map<Long, Double> dps2 = new HashMap<>();

        dps1.put(1L, 2d);
        dps2.put(1L, 3d);
        dps1.put(2L, 3d);
        dps2.put(10l, 10.3);

        dps1 = aggregator.aggregate(dps1, dps2);
        System.out.println(dps1);

    }
}
