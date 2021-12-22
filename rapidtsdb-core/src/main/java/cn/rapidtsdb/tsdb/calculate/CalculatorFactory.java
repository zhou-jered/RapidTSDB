package cn.rapidtsdb.tsdb.calculate;


import cn.rapidtsdb.tsdb.common.TimeUtils;

public class CalculatorFactory {

    public static Aggregator getAggregator(String aggregatorName) {
        LinearFunction func = getFunction(aggregatorName);
        return new LinearAggregator(func);
    }

    /**
     * config like 10s-avg
     * 1m-sum  10m-count
     *
     * @param downSamplerConfig
     * @return
     */
    public static DownSampler getDownSample(String downSamplerConfig) {
        if (downSamplerConfig == null) {
            return null;
        }
        String[] timeRangeAndName = downSamplerConfig.split("\\-");
        if (timeRangeAndName.length != 2) {
            throw new RuntimeException("Unknown downsampler config " + downSamplerConfig);
        }
        String timeRangeConfig = timeRangeAndName[0];
        String dsName = timeRangeAndName[1];
        int timeRangeMills = TimeUtils.parseMillsConfig(timeRangeConfig);
        LinearFunction downSampler = getFunction(dsName);
        LinearDownSampler linearDownSampler = new LinearDownSampler(timeRangeMills, downSampler);
        return linearDownSampler;
    }


    private static LinearFunction getFunction(String name) {
        name = name.trim();
        switch (name.toLowerCase()) {
            case "max":
                return LinearFunctions.maxer;
            case "min":
                return LinearFunctions.miner;
            case "sum":
                return LinearFunctions.sumer;
            case "count":
                return LinearFunctions.counter;
            case "ave":
            case "avg":
            case "average":
                return LinearFunctions.average;
            case "dev":
                return LinearFunctions.dever;
            case "var":
                return LinearFunctions.var;
            case "cov":
                return LinearFunctions.cov;
            default:
                throw new RuntimeException("Unknown down sampler name [" + name + "], see docs for more information.");
        }
    }

}
