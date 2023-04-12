package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.replication.Replicator.State;
import com.ascherbakoff.ai3.replication.Request;
import java.lang.System.Logger.Level;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The leasholder is a standalone node.
 */
public class ReplicationGroup3NodesTest extends BasicReplicationTest {
    private static System.Logger LOGGER = System.getLogger(ReplicationGroup3NodesTest.class.getName());

    @Override
    protected void createCluster() {
        createCluster(3);
    }

    @Test
    public void testBasicReplication() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(leader);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
        leaseholder.sync(GRP_NAME).join();

        for (Node node : top.getNodeMap().values()) {
            assertEquals(0, node.localGet(GRP_NAME, 0, node.group(GRP_NAME).lwm).join());
        }
    }

    @Test
    public void testSendConcurrent() throws InterruptedException {
        createCluster();

        Executor senderPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        int msgCnt = 1000;

        AtomicInteger gen = new AtomicInteger();
        AtomicInteger errCnt = new AtomicInteger();
        CountDownLatch l = new CountDownLatch(msgCnt);

        long ts = System.nanoTime();

        while (msgCnt-- > 0) {
            senderPool.execute(() -> {
                int val = gen.incrementAndGet();
                top.getNode(leader).replicate(GRP_NAME, new Put(val, val)).exceptionally(err -> {
                    errCnt.incrementAndGet();
                    LOGGER.log(Level.ERROR, "Failed to replicate", err);
                    return null;
                }).thenAccept(r -> l.countDown());
            });
        }

        l.await();

        assertEquals(0, l.getCount());
        assertEquals(0, errCnt.get());

        LOGGER.log(Level.INFO, "Finished sending messages, duration {0}ms", (System.nanoTime() - ts) / 1000 / 1000.);

        Timestamp lwm0 = top.getNode(leader).group(GRP_NAME).lwm;

        for (int i = 0; i < msgCnt; i++) {
            for (Node node : top.getNodeMap().values()) {
                assertEquals(i, node.localGet(GRP_NAME, i, lwm0).join());
            }
        }
    }

    @Test
    public void testLeaseholderFailure() {
        createCluster();
    }

    /**
     * 1. One of replication messages is delayed infinitely.
     * <p>Expected result: operation is failed after some timeout, closing the gap only for successful majority of replicators.
     * Note: failed replicator will eventually be switched to error mode. to recover from error mode, the replicator must periodically
     * attempt to trigger catch up state on outdated replica.
     */
    @Test
    public void testClosedGaps() throws Exception {
        createCluster();

        Node leaseholder = top.getNode(leader);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.

        leaseholder.sync(GRP_NAME).join();

        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getPayload() instanceof Replicate);

        val++;
        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        fut.join();

        // LWM shoudn't propagate for alice->bob until infligh is not completed.
        leaseholder.sync(GRP_NAME).join();

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        Timestamp t1 = top.getNode(alice).group(GRP_NAME).lwm;
        assertEquals(t0, t1);
        Timestamp t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;
        assertEquals(t2, t3);
        Timestamp t4 = top.getNode(alice).group(GRP_NAME).replicators.get(charlie).getLwm();
        Timestamp t5 = top.getNode(charlie).group(GRP_NAME).lwm;
        assertEquals(t4, t5);
        assertTrue(t0.compareTo(t2) > 0);
        assertTrue(t1.compareTo(t3) > 0);
        assertTrue(t0.equals(t4));
        assertTrue(t1.equals(t5));

        Request request = toBob.client().blocked().get(0);
        Inflight inflight = top.getNode(alice).group(GRP_NAME).replicators.get(bob).inflight(request.getTs());
        inflight.future().join();
        assertTrue(inflight.state() == State.ERROR);

        leaseholder.sync(GRP_NAME).join();

        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        t1 = top.getNode(alice).group(GRP_NAME).lwm;
        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        t3 = top.getNode(bob).group(GRP_NAME).lwm;
        t4 = top.getNode(alice).group(GRP_NAME).replicators.get(charlie).getLwm();
        t5 = top.getNode(charlie).group(GRP_NAME).lwm;

        assertTrue(t0.equals(t1));
        assertTrue(t0.compareTo(t2) > 0);
        assertTrue(t1.compareTo(t3) > 0);
        assertTrue(t0.equals(t4));
        assertTrue(t1.equals(t5));

        toBob.client().clearBlock();

        val++;
        CompletableFuture<Timestamp> fut2 = leaseholder.replicate(GRP_NAME, new Put(val, val));
        fut2.join();

        leaseholder.sync(GRP_NAME).join();

        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        t1 = top.getNode(alice).group(GRP_NAME).lwm;
        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        t3 = top.getNode(bob).group(GRP_NAME).lwm;
        t4 = top.getNode(alice).group(GRP_NAME).replicators.get(charlie).getLwm();
        t5 = top.getNode(charlie).group(GRP_NAME).lwm;
        assertTrue(t0.equals(t1));
        assertTrue(t0.compareTo(t2) > 0);
        assertTrue(t1.compareTo(t3) > 0);
        assertTrue(t0.equals(t4));
        assertTrue(t1.equals(t5));
    }

    /**
     * Tests if node with non max lwm can become the leader.
     */
    @Test
    public void testAssignNotUpToDate() {
        createCluster();

        int val = 0;
        Node leaseholder = top.getNode(leader);
        Timestamp ts = leaseholder.replicate(GRP_NAME, new Put(val, val)).join();// Init replicators.

        for (Node value : top.getNodeMap().values()) {
            assertEquals(val, value.localGet(GRP_NAME, val, ts).join());
        }

        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getPayload() instanceof Replicate);

        val++;
        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        fut.join(); // The request must succeed on alice and charlie
    }

    /**
     * Tests if a lease can always be refreshed if current holder is available.
     */
    @Test
    public void testRefreshNoMajority() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        assertNotNull(top.getNodeMap().remove(bob));
        assertNotNull(top.getNodeMap().remove(charlie));

        Timestamp ts = tracker.assignLeaseholder(GRP_NAME, leader, nodeIds).join();
        waitLeaseholder(ts, leader, tracker, top, GRP_NAME);

        validateLease(leader);

        Node leaseholder = top.getNode(leader);

        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(0, 0));
        assertThrows(CompletionException.class, () -> fut.join(), "Replication must fail");
    }

    /**
     * Tests if a lease can't be reassigned if a majority is not avaiable.
     */
    @Test
    public void testReassignNoMajority() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        assertNotNull(top.getNodeMap().remove(bob));
        assertNotNull(top.getNodeMap().remove(charlie));

        assertThrows(CompletionException.class, () -> tracker.assignLeaseholder(GRP_NAME, leader, nodeIds).join(), "Election must fail");
    }

    @Test
    public void testReassignEmpty() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        assertNotNull(top.getNodeMap().remove(alice));
        assertNotNull(top.getNodeMap().remove(bob));
        assertNotNull(top.getNodeMap().remove(charlie));

        assertThrows(CompletionException.class, () -> tracker.assignLeaseholder(GRP_NAME, leader, nodeIds).join(), "Can't assign on empty group");
    }


    /**
     * Tests the scenario:
     * 3 node group, leader was lost.
     * On next leader election, previos leader was removed from group.
     */
    @Test
    public void testReconfigurationOldLeaderRemoved() {

    }

    /**
     * Tests replication group size change.
     */
    @Test
    public void testReconfigurationUpscale() {
        createCluster();

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
    }

    @Test
    public void testReconfigurationDownscale() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();

        Set<NodeId> newMembers = Set.of(alice);

        Timestamp ts = tracker.assignLeaseholder(GRP_NAME, leader, newMembers).join();

        for (NodeId nodeId : newMembers) {
            Group locGroup = top.getNode(nodeId).group(GRP_NAME);
            assertTrue(waitForCondition(() -> newMembers.equals(locGroup.getMembers()), 1000), nodeId.toString());
        }
    }

}
