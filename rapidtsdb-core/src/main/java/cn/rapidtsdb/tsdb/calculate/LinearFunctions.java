package cn.rapidtsdb.tsdb.calculate;

public class LinearFunctions {
    public static final LinearFunction sumer = (params) -> {
        double result = 0;
        for (double p : params) {
            result += p;
        }
        return result;
    };


    public static final LinearFunction maxer = (params) -> {
        double result = Double.MIN_VALUE;
        for (double p : params) {
            result = Math.max(result, p);
        }
        return result;
    };

    public static final LinearFunction miner = (params) -> {
        double result = Double.MAX_VALUE;
        for (double p : params) {
            result = Math.min(result, p);
        }
        return result;
    };

    public static final LinearFunction counter = (params -> params.size());

    public static final LinearFunction average = (params) -> sumer.apply(params) / counter.apply(params);

    public static final LinearFunction dever = params -> {
        double avg = average.apply(params);
        double result = 0;
        for (double p : params) {
            result += (avg - p) * (avg - p);
        }
        return Math.sqrt(result);
    };

    public static final LinearFunction var = params -> {
        return 1;
    };

    public static final LinearFunction cov = params -> {
        return 1;
    };

}
