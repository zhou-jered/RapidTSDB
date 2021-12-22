package cn.rapidtsdb.tsdb.calculate;

import cn.rapidtsdb.tsdb.utils.CollectionUtils;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;

/**
 * todo add data miss strategy
 */
public class LinearAggregator implements Aggregator {
    LinearFunction func;

    public LinearAggregator(LinearFunction func) {
        this.func = func;
    }

    @Override
    public Map<Long, Double> aggregate(Map<Long, Double> dps1, Map<Long, Double> dps2) {
        if (CollectionUtils.isEmpty(dps1)) {
            return dps2;
        }
        if (CollectionUtils.isEmpty(dps2)) {
            return dps1;
        }
        Map<Long, Double> result = new HashMap<>(dps1);
        dps2.forEach((t, v) -> {
            result.compute(t, (ok, ov) ->
                    ov == null ? (v) : (func.apply(Lists.newArrayList(ov, v)))
            );
        });
        return result;
    }
}
