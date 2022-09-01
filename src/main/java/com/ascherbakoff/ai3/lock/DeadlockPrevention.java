package com.ascherbakoff.ai3.lock;

public class DeadlockPrevention {
    boolean forceOrder;

    private DeadlockPrevention() {
    }

    /**
     * Causes a low priority(higher timestamp) transaction to abort if a higher priority(lower priority) transaction holds the lock.
     * Aborted transaction must restart with the same timestamp.
     *
     * @return The policy.
     */
    public static DeadlockPrevention waitDie() {
        DeadlockPrevention d = new DeadlockPrevention();
        d.forceOrder = true;
        return d;
    }

    public static DeadlockPrevention none() {
        return new DeadlockPrevention();
    }
}
