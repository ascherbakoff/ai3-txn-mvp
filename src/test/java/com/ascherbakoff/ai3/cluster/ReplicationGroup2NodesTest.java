package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.replication.Request;
import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
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
public class ReplicationGroup2NodesTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(ReplicationGroup2NodesTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;
    NodeId leader;

    private void createCluster() {
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top, clock));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top, clock));

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        leader = alice;

        tracker = new Tracker(top, clock);
        tracker.register(GRP_NAME, nodeIds);
        tracker.assignLeaseholder(GRP_NAME, leader);

        waitLeaseholder(leader);
    }

    private void waitLeaseholder(NodeId nodeId) {
        assertEquals(nodeId, tracker.getLeaseHolder(GRP_NAME));

        Timestamp ts = tracker.clock().get();

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                NodeId leaseHolder = node.getLeaseHolder(GRP_NAME);
                Timestamp lease = node.getLease(GRP_NAME);
                if (leaseHolder == null || lease == null)
                    return false;
                return nodeId.equals(leaseHolder) && ts.compareTo(lease) >= 0;
            }, 1_000));
        }
    }

    @Test
    public void testBasicReplication() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
        validate(0);
    }

    @Test
    public void testIdlePropagation() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
        adjustClocks(20);

        validateAtTimestamp(0, clock.get());
    }

    @Test
    public void testLwmPropagation() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();

        Timestamp t2 = leaseholder.group(GRP_NAME).replicators.get(leader).getLwm();
        Timestamp t3 = leaseholder.group(GRP_NAME).replicators.get(leader == alice ? bob : alice).getLwm();
        Timestamp t4 = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t5 = top.getNode(bob).group(GRP_NAME).lwm;

        assertTrue(t2.compareTo(t4) == 0);
        assertTrue(t3.compareTo(t5) > 0);

        leaseholder.sync(GRP_NAME).join();

        assertEquals(leaseholder.group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
        assertEquals(leaseholder.group(GRP_NAME).replicators.get(bob).getLwm(), top.getNode(bob).group(GRP_NAME).lwm);
    }

    @Test
    public void testLwmPropagationOutOfOrder() throws InterruptedException {
        createCluster();

        top.getNode(alice).createReplicator(GRP_NAME, bob);
        Replicator toBob = top.getNode(alice).group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();

        CompletableFuture<Timestamp> res0 = top.getNode(alice).replicate(GRP_NAME, new Put(0, 0));
        CompletableFuture<Timestamp> res1 = top.getNode(alice).replicate(GRP_NAME, new Put(1, 1));
        CompletableFuture<Timestamp> res2 = top.getNode(alice).replicate(GRP_NAME, new Put(2, 2));

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 3, 1000));
        ArrayList<Request> blocked = toBob.client().blocked();

        toBob.client().unblock(r -> r.getTs().equals(blocked.get(2).getTs()));
        assertTrue(waitForCondition(() -> toBob.inflight(blocked.get(2).getTs()).future().isDone(), 1000));
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().unblock(r -> r.getTs().equals(blocked.get(1).getTs()));
        assertTrue(waitForCondition(() -> toBob.inflight(blocked.get(1).getTs()).future().isDone(), 1000));
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        Inflight inf = toBob.inflight(blocked.get(0).getTs());
        toBob.client().unblock(r -> r.getTs().equals(blocked.get(0).getTs()));
        assertTrue(waitForCondition(() -> inf.future().isDone(), 1000));
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        res0.join();
        res1.join();
        res2.join();

        assertEquals(0, toBob.inflights());

        Timestamp t = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        toBob.client().clearBlock();
        top.getNode(alice).sync(GRP_NAME).join();

        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm(), top.getNode(bob).group(GRP_NAME).lwm);
        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
    }

    @Test
    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
        createCluster();

        top.getNode(alice).createReplicator(GRP_NAME, bob);
        Replicator toBob = top.getNode(alice).group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();

        CompletableFuture<Timestamp> res0 = top.getNode(alice).replicate(GRP_NAME, new Put(0, 0));
        CompletableFuture<Timestamp> res1 = top.getNode(alice).replicate(GRP_NAME, new Put(1, 1));

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 2, 1000));
        ArrayList<Request> blocked = toBob.client().blocked();

        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().unblock(r -> r.getTs().equals(blocked.get(0).getTs()));
        toBob.inflight(blocked.get(0).getTs()).future().join();
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm); // LWM was not propagated by subsequent message.

        CompletableFuture<Timestamp> res2 = top.getNode(alice).replicate(GRP_NAME, new Put(2, 2));

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 2, 1000));
        ArrayList<Request> blocked2 = toBob.client().blocked();

        Inflight i2 = toBob.inflight(blocked2.get(1).getTs());
        toBob.client().unblock(r -> r.getTs().equals(i2.ts()));
        assertTrue(waitForCondition(() -> i2.future().isDone(), 1000));
        assertEquals(blocked.get(0).getTs(), top.getNode(bob).group(GRP_NAME).lwm);

        Inflight i1 = toBob.inflight(blocked2.get(0).getTs());
        toBob.client().unblock(r -> r.getTs().equals(blocked2.get(0).getTs()));
        assertTrue(waitForCondition(() -> i1.future().isDone(), 1000));
        assertEquals(blocked.get(0).getTs(), top.getNode(bob).group(GRP_NAME).lwm);

        res0.join();
        res1.join();
        res2.join();

        Timestamp t = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        assertEquals(0, toBob.inflights());

        toBob.client().clearBlock();
        top.getNode(alice).sync(GRP_NAME).join();

        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm(), top.getNode(bob).group(GRP_NAME).lwm);
        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
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

        while(msgCnt-- > 0) {
            senderPool.execute(() -> {
                int val = gen.incrementAndGet();
                top.getNode(alice).replicate(GRP_NAME, new Put(val, val)).exceptionally(err -> {
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

        top.getNode(alice).sync(GRP_NAME).join();

        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
        assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm(), top.getNode(bob).group(GRP_NAME).lwm);
    }

    @Test
    public void testLeaseholderFailure() {
        createCluster();
    }

    /**
     * Tests if messages from invalid leaseholder are ignored.
     */
    @Test
    public void testOutdatedReplication() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        validate(val);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        validate(val);

        val++;
        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertFalse(fut.isDone());

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));

        adjustClocks(Tracker.LEASE_DURATION / 2);

        toBob.client().unblock(r -> true);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    @Test
    public void testOutdatedReplication2() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(leader);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        validate(val);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        validate(val);

        val++;
        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertFalse(fut.isDone());

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));

        adjustClocks(Tracker.LEASE_DURATION / 2 + Tracker.MAX_CLOCK_SKEW);

        // Re-elect.
        assertTrue(tracker.assignLeaseholder(GRP_NAME, bob));
        waitLeaseholder(bob);

        toBob.client().unblock(r -> true);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    /**
     * 1. One of replication messages is delayed until it's epoch is expired (the epoch corresponds to a lease duration of the initiating node).
     * <p>2. After expiration message is delivered.
     * <p>Expected result: operation is failed.
     * Note: this outcome seems ok, because the lease is refreshed each LEASE_DURATION / 2 and the message RTT should typically be much less.
     */
    @Test
    public void testOutdatedReplication3() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        Timestamp oldLease = tracker.getLease(GRP_NAME);
        assertNotNull(oldLease);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.

        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getTs().physical() < Tracker.LEASE_DURATION / 2);

        val++;
        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        assertFalse(fut.isDone());

        adjustClocks(Tracker.LEASE_DURATION / 2);

        tracker.refreshLeaseholder(GRP_NAME);
        waitLeaseholder(leader);

        adjustClocks(Tracker.LEASE_DURATION / 2 + Tracker.MAX_CLOCK_SKEW);

        toBob.client().unblock(r -> true);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    /**
     * 1. One of replication messages is delayed infinitely.
     * <p>Expected result: operation is failed after some timeout, closing the gap on all replicators.
     * Note: not closing gaps leads to prevention of safeTime propagation.
     */
    @Test
    public void testClosedGaps() throws Exception {
        createCluster();

        Node leaseholder = top.getNode(leader);
        Timestamp oldLease = tracker.getLease(GRP_NAME);
        assertNotNull(oldLease);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.

        leaseholder.sync(GRP_NAME).join();

        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getPayload() instanceof Replicate);

        val++;
        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        assertFalse(fut.isDone());

        Thread.sleep(200); // Give time to finish for alice->alice replication.

        // LWM shoudn't propagate for alice->bob until infligh is not completed.
        leaseholder.sync(GRP_NAME).join();

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        Timestamp t1 = top.getNode(alice).group(GRP_NAME).lwm;
        assertEquals(t0, t1);
        Timestamp t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;
        assertEquals(t2, t3);
        assertTrue(t0.compareTo(t2) > 0);
        assertTrue(t1.compareTo(t3) > 0);

        assertThrows(CompletionException.class, () -> fut.join());

        leaseholder.sync(GRP_NAME).join();

        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        t1 = top.getNode(alice).group(GRP_NAME).lwm;
        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        t3 = top.getNode(bob).group(GRP_NAME).lwm;
        assertEquals(t0, t1);
        assertEquals(t1, t2);
        assertEquals(t2, t3);

        toBob.client().clearBlock();

        val++;
        CompletableFuture<Timestamp> fut2 = leaseholder.replicate(GRP_NAME, new Put(val, val));
        fut2.join();

        leaseholder.sync(GRP_NAME).join();

        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        t1 = top.getNode(alice).group(GRP_NAME).lwm;
        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        t3 = top.getNode(bob).group(GRP_NAME).lwm;
        assertEquals(t0, t1);
        assertEquals(t1, t2);
        assertEquals(t2, t3);
    }

    /**
     * 1. One of replication messages is delayed infinitely.
     * <p>2. Many replication commands are issued, causing inflights overflow.
     * <p>Expected result: on overflow the replicator wents to error state, preventing any replication activity.
     * The correposnding node must be restarted to re-create replicator and perform catch-up.
     * Note: the same behavior must be applied on replication command uncaught exception.
     */
    @Test
    public void testReplicatorErrorOnOverflow() {
        fail();
    }

    @Test
    public void testReplicatorErrorOnTimeout() {
        fail();
    }

    @Test
    public void testReplicatorErrorOnBadResponse() {
        fail();
    }

    @Test
    public void testGroupInErrorStateCannotBecomeLeader() {
        fail();
    }

    /**
     * Tests if a message containing illegal ts value (in the future) is ignored.
     */
    @Test
    public void testBrokenClocks() {
        fail();
    }

    private void validate(int val) {
        Node leaseholder = top.getNode(leader);
        leaseholder.sync(GRP_NAME).join();
        assertEquals(val, top.getNode(leader).localGet(GRP_NAME, val, top.getNode(alice).group(GRP_NAME).lwm));
        assertEquals(val, top.getNode(leader).localGet(GRP_NAME, val, top.getNode(bob).group(GRP_NAME).lwm));
    }

    private void validateAtTimestamp(int val, Timestamp ts) {
        Node leaseholder = top.getNode(leader);
        leaseholder.sync(GRP_NAME).join();
        assertEquals(val, top.getNode(leader).localGet(GRP_NAME, val, ts));
        assertEquals(val, top.getNode(leader).localGet(GRP_NAME, val, ts));
    }
}
