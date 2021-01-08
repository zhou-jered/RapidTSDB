package cn.rapidtsdb.tsdb.calculate;

@FunctionalInterface
public interface LinerFunction {
    double apply(double... params);
}
