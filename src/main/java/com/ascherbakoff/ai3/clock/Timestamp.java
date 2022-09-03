package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;

public class Timestamp implements Comparable<Timestamp> {
    private static AtomicLong seq = new AtomicLong();
    private final long ts;

    Timestamp(long ts) {
        this.ts = ts;
    }

    @Override
    public int compareTo(@NotNull Timestamp o) {
        return Long.compare(ts, o.ts);
    }

    public static Timestamp now() {
        return new Timestamp(seq.incrementAndGet());
    }
}
