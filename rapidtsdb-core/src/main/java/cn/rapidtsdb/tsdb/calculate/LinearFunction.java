package cn.rapidtsdb.tsdb.calculate;

import java.util.Collection;

@FunctionalInterface
public interface LinearFunction {
    double apply(Collection<Double> params);
}
