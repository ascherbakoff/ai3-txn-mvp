package com.ascherbakoff.ai3.clock;

import org.jetbrains.annotations.TestOnly;

public class Clock {
    private final PhysicalTimeProvider provider;

    private Timestamp current = new Timestamp(0, 0);

    public Clock(PhysicalTimeProvider provider) {
        this.provider = provider;
    }

    public synchronized Timestamp now() {
        long physNew = provider.get();

        if (physNew > current.physical())
            current = new Timestamp(physNew, 0);
        else
            current = new Timestamp(current.physical(), current.counter() + 1);

        return current;
    }

    @TestOnly
    public synchronized Timestamp get() {
        return current;
    }

    public synchronized void onRequest(Timestamp ts) {
        long physNew = provider.get();
        long logic = current.counter();

        if (ts.physical() != physNew) {
            current = new Timestamp(Math.max(physNew, ts.physical()), 0);
        } else {
            if (ts.counter() != logic) {
                current = new Timestamp(physNew, Math.max(ts.counter(), logic));
            }
        }
    }

    public synchronized void onResponse(Timestamp ts) {
        onRequest(ts);
    }

    @Override
    public synchronized String toString() {
        return "Clock{" +
                "now=" + current +
                '}';
    }
}
