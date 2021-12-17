package cn.rapidtsdb.tsdb.calculate;

@FunctionalInterface
public interface BiFunction {
    double consume(double val1, double val2);
}
