package com.ascherbakoff.ai3.util;

import static org.junit.jupiter.api.Assertions.fail;

import com.ascherbakoff.ai3.clock.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class BasicTest {
    protected Clock clock = new Clock();

    private AtomicInteger idGen = new AtomicInteger();

    protected boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long ts = System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() < ts) {
            try {
                if (cond.getAsBoolean())
                    return true;

                Thread.sleep(50);
            } catch (InterruptedException e) {
                fail("Failed to wait for condition");
            }
        }

        return false;
    }

    protected UUID nextId() {
        return new UUID(0, idGen.incrementAndGet());
    }
}
