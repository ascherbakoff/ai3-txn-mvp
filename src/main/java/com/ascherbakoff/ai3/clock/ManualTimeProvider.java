package com.ascherbakoff.ai3.clock;

import java.util.concurrent.atomic.AtomicLong;

public class ManualTimeProvider implements PhysicalTimeProvider {
    private AtomicLong val = new AtomicLong();

    @Override
    public long get() {
        return val.get();
    }

    public void set(long value) {
        val.set(value);
    }

    public void adjust(long delta) {
        val.addAndGet(delta);
    }
}
