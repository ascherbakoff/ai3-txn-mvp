package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

public class LeaderAssignTest extends BasicReplicationTest {
    private static System.Logger LOGGER = System.getLogger(LeaderAssignTest.class.getName());

    @Test
    public void testAssign() {
        createCluster();
    }

    @Test
    public void testBadAssign() {
        createCluster();

        assertThrows(CompletionException.class, () -> tracker.assignLeader(GRP_NAME, bob, nodeIds).join());

        validate(alice);
    }

    @Test
    public void testExpire() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION);

        validate(null);
    }

    @Test
    public void testRefresh() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        // Refresh.
        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, nodeIds).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        // Lease still active after lease duration
        waitLeader(ts, alice, tracker, top, GRP_NAME);
    }

    @Test
    public void testReassign() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        Timestamp ts = tracker.assignLeader(GRP_NAME, bob, nodeIds).join();
        waitLeader(ts, bob, tracker, top, GRP_NAME);

        validate(bob);
    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder.
     * 2. Tracker sends the message but dies before receiving an ack, so leaseholder remains unknown.
     * 3. Tracker is restarted.
     * 4. The tracker attempts to assign a leaseholder after the current lease expires.
     *
     * Expected result: only one leaseholder exists.
     *
     */
    @Test
    public void testScenario1() {

    }

    /**
     * 1. Tracker attempts to assing some node as a leaseholder.
     * 2. Tracker sends the message to a leaseholder, but the message is delayed for t > lease duration.
     * 3. Would-be leaseholder eventually receives the message.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario2() {

    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder
     * 2. Tracker delivers the message to a leaseholder, but the ack is not received due to partition (leasholder node remains alive), so leaseholder remains unknown.
     * 3. Partition condition is healed, but message is lost.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario3() {

    }

    /**
     * 1. Tracker attempts to assing some node as leaseholder
     * 2. Tracker sends the message to a leaseholder, but it was not received due to partition (leasholder node remains alive), so leaseholder remains unknown.
     * 3. Partition condition is healed, but message is lost.
     * 4. Tracker attempts to assign a new leaseholder after the timeout.
     *
     * Expected result: only one leaseholder exist.
     *
     */
    @Test
    public void testScenario4() {

    }
}
