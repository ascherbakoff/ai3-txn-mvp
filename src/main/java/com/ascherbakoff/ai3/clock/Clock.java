package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;

public class Clock {
    private AtomicLong seq = new AtomicLong();

    private Timestamp now = Timestamp.min();

    public synchronized Timestamp now() {
        return now;
    }

    public synchronized Timestamp tick() {
        now = new Timestamp(seq.incrementAndGet());
        if (seq.get() > 100) {
            System.out.println();
        }
        return now;
    }

    public synchronized void onRequest(Timestamp ts) {
        if (ts.compareTo(now) > 0) {
            seq.set(ts.counter());
            now = new Timestamp(ts.counter());
        }
    }

    public synchronized void onResponse(Timestamp ts) {
        onRequest(ts);
    }

    @Override
    public String toString() {
        return "Clock{" +
                "now=" + now +
                '}';
    }
}
