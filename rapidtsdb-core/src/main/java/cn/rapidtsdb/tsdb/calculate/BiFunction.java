package cn.rapidtsdb.tsdb.calculate;

import org.checkerframework.common.value.qual.DoubleVal;

@FunctionalInterface
public interface BiFunction {
    double consume(double val1, DoubleVal val2);
}
