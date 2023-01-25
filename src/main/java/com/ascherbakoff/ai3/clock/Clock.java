package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;

public class Clock {
    private AtomicLong seq = new AtomicLong();

    private Timestamp now = Timestamp.min();

    public Timestamp now() {
        return now;
    }

    public Timestamp tick() {
        now = new Timestamp(seq.incrementAndGet());
        return now;
    }

    public void onRequest(Timestamp ts) {
        if (ts.compareTo(now) > 0) {
            seq.set(ts.counter());
            now = new Timestamp(ts.counter());
        } else {
            now = new Timestamp(seq.incrementAndGet());
        }
    }

    public void onResponse(Timestamp ts) {
        if (ts.compareTo(now) > 0) {
            seq.set(ts.counter());
            now = new Timestamp(ts.counter());
        }
    }

    @Override
    public String toString() {
        return "Clock{" +
                "now=" + now +
                '}';
    }
}
