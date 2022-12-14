package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;

public class Timestamp implements Comparable<Timestamp> {
    private static AtomicLong seq = new AtomicLong();
    private final long counter;

    Timestamp(long counter) {
        this.counter = counter;
    }

    @Override
    public int compareTo(@NotNull Timestamp o) {
        return Long.compare(counter, o.counter);
    }

    public static Timestamp now() {
        return new Timestamp(seq.incrementAndGet());
    }

    @Override
    public String toString() {
        return "Timestamp{" +
                "counter=" + counter +
                '}';
    }
}
