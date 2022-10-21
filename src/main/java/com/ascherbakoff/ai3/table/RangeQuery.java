package com.ascherbakoff.ai3.table;

import java.util.concurrent.CyclicBarrier;
import org.jetbrains.annotations.Nullable;

public class RangeQuery implements Query {
    public @Nullable CyclicBarrier delayOnNext;
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
