package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * The leaseholder is colocated to alice replica.
 */
public class ReplicationGroupColocatedTest extends ReplicationGroupTest {
    @Override
    protected boolean colocated() {
        return true;
    }

    @Override
    @Test
    public void testLwmPropagationOutOfOrder() throws InterruptedException {
        super.testLwmPropagationOutOfOrder();

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
    }

    @Override
    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
        super.testLwmPropagationOutOfOrder2();

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
    }

    @Override
    public void testSendConcurrent() throws InterruptedException {
        super.testSendConcurrent();

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
    }
}
