package com.ascherbakoff.ai3.cluster;

/**
 * The leaseholder is colocated to alice replica.
 */
public class ReplicationGroupColocatedTest extends ReplicationGroupTest {
    @Override
    protected boolean colocated() {
        return true;
    }
}
