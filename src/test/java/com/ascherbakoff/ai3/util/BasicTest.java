package com.ascherbakoff.ai3.util;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.BooleanSupplier;

public class BasicTest {
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
}
