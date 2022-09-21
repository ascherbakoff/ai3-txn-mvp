package com.ascherbakoff.ai3.table;

public class RangeQuery implements Query {
    int col;
    Tuple lowerKey;
    boolean lowerInclusive;
    Tuple upperKey;
    boolean upperInclusive;

    public RangeQuery(int col, Tuple lowerKey, boolean lowerInclusive, Tuple upperKey, boolean upperInclusive) {
        this.col = col;
        this.lowerKey = lowerKey;
        this.lowerInclusive = lowerInclusive;
        this.upperKey = upperKey;
        this.upperInclusive = upperInclusive;
    }

    public RangeQuery(EqQuery query0) {
        this(query0.col, query0.queryKey, true, query0.queryKey, true);
    }
}
