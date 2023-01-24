package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;

public class Timestamp implements Comparable<Timestamp> {
    private static AtomicLong seq = new AtomicLong();
    private final long counter;

    Timestamp(long counter) {
        this.counter = counter;
    }

    public long getCounter() {
        return counter;
    }

    @Override
    public int compareTo(@NotNull Timestamp o) {
        return Long.compare(counter, o.counter);
    }

    public Timestamp adjust(long delta) {
        return new Timestamp(counter + delta);
    }

    public static Timestamp now() {
        return new Timestamp(seq.incrementAndGet());
    }

    public static Timestamp min() {
        return new Timestamp(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Timestamp timestamp = (Timestamp) o;

        if (counter != timestamp.counter) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (counter ^ (counter >>> 32));
    }

    @Override
    public String toString() {
        return "Timestamp{" +
                "counter=" + counter +
                '}';
    }
}
