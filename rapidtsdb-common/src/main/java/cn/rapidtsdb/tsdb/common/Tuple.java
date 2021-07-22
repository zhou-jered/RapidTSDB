package cn.rapidtsdb.tsdb.common;

import lombok.Data;

import java.util.Objects;

@Data
public class Tuple<L, M, R> {
    L left;
    M middle;
    R right;

    public Tuple(L left, M middle, R right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tuple<?, ?, ?> tuple = (Tuple<?, ?, ?>) o;
        return Objects.equals(left, tuple.left) &&
                Objects.equals(middle, tuple.middle) &&
                Objects.equals(right, tuple.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, middle, right);
    }
}
