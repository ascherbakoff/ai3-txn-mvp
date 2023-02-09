package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node.Result;
import com.ascherbakoff.ai3.replication.Command;
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
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * The leasholder is a standalone node.
 */
public class ReplicationGroupTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(ReplicationGroupTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;
    NodeId holder;

    protected boolean colocated() {
        return false;
    }

    private void createCluster() {
        createCluster(colocated());
    }

    private void createCluster(boolean colocated) {
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top, clock));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top, clock));

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        tracker = new Tracker(top, clock);
        tracker.register(GRP_NAME, nodeIds);
        if (colocated) {
            holder = alice;
            tracker.assignLeaseholder(GRP_NAME, holder);
        } else {
            holder = new NodeId("holder");
            top.regiser(new Node(holder, top, clock));

            tracker.assignLeaseholder(GRP_NAME, holder);
        }

        waitLeaseholder(holder);
    }

    private void waitLeaseholder(NodeId nodeId) {
        assertEquals(nodeId, tracker.getLeaseHolder(GRP_NAME));

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> nodeId.equals(node.getLeaseHolder(GRP_NAME)), 1_00000));
        }
    }

    @Test
    public void testBasicReplication() {
        createCluster();

        Node leaseholder = top.getNode(holder);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).future().join();

        assertEquals(0, top.getNode(alice).get(GRP_NAME, 0, top.getNode(alice).clock().get()));
        assertEquals(0, top.getNode(bob).get(GRP_NAME, 0, top.getNode(bob).clock().get()));
    }

    @Test
    public void testLwmPropagation() {
        createCluster();

        top.getNode(holder).replicate(GRP_NAME, new Put(0, 0)).future().join();

        Timestamp t2 = top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm();
        Timestamp t3 = top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t4 = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t5 = top.getNode(bob).group(GRP_NAME).lwm;

        assertTrue(t2.compareTo(t4) >= 0); // = for colocated case.
        assertTrue(t3.compareTo(t5) > 0);

        top.getNode(holder).sync(GRP_NAME).join();

        Timestamp t6 = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t7 = top.getNode(bob).group(GRP_NAME).lwm;

        assertTrue(t2.compareTo(t6) == 0);
        assertTrue(t3.compareTo(t7) == 0);
    }

    @Test
    public void testLwmPropagationOutOfOrder() throws InterruptedException {
        createCluster();

        top.getNode(holder).createReplicator(GRP_NAME, bob);
        Replicator toBob = top.getNode(holder).group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        Timestamp t0 = top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm();

        Result res0 = top.getNode(holder).replicate(GRP_NAME, new Put(0, 0));
        Result res1 = top.getNode(holder).replicate(GRP_NAME, new Put(1, 1));
        Result res2 = top.getNode(holder).replicate(GRP_NAME, new Put(2, 2));

        toBob.client().stopBlock(r -> r.getTs().equals(res2.getPending().get(bob).ts()));
        waitForCondition(() -> res2.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().stopBlock(r -> r.getTs().equals(res1.getPending().get(bob).ts()));
        waitForCondition(() -> res1.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().stopBlock(r -> r.getTs().equals(res0.getPending().get(bob).ts()));
        waitForCondition(() -> res0.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        res1.future().join();
        res1.future().join();
        res1.future().join();

        assertEquals(0, toBob.inflights());

        Timestamp t = top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        toBob.client().clearBlock();
        top.getNode(holder).sync(GRP_NAME).join();

        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t, t3);

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
    }

    @Test
    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
        createCluster();

        top.getNode(holder).createReplicator(GRP_NAME, bob);
        Replicator toBob = top.getNode(holder).group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        Timestamp t0 = top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm();

        Result res1 = top.getNode(holder).replicate(GRP_NAME, new Put(0, 0));
        Result res2 = top.getNode(holder).replicate(GRP_NAME, new Put(1, 1));

        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().stopBlock(r -> r.getTs().equals(res1.getPending().get(bob).ts()));
        res1.future().join();
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        Result res3 = top.getNode(holder).replicate(GRP_NAME, new Put(3, 3));
        Inflight i3 = res3.getPending().get(bob);
        toBob.client().stopBlock(r -> r.getTs().equals(i3.ts()));
        assertTrue(waitForCondition(() -> i3.isAcked(), 1000));
        assertEquals(res1.getPending().get(bob).ts(), top.getNode(bob).group(GRP_NAME).lwm);

        toBob.client().stopBlock(r -> r.getTs().equals(res2.getPending().get(bob).ts()));
        assertTrue(waitForCondition(() -> res2.getPending().get(bob).isAcked(), 1000));
        assertEquals(res1.getPending().get(bob).ts(), top.getNode(bob).group(GRP_NAME).lwm);

        res1.future().join();
        res2.future().join();
        res3.future().join();

        Timestamp t = top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        assertEquals(0, toBob.inflights());

        toBob.client().clearBlock();
        top.getNode(holder).sync(GRP_NAME).join();

        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t, t3);

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
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
                top.getNode(holder).replicate(GRP_NAME, new Put(val, val)).future().exceptionally(err -> {
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

        top.getNode(holder).sync(GRP_NAME).join();

        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(alice).getLwm(), top.getNode(alice).group(GRP_NAME).lwm);
        assertEquals(top.getNode(holder).group(GRP_NAME).replicators.get(bob).getLwm(), top.getNode(bob).group(GRP_NAME).lwm);
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

        Node leaseholder = top.getNode(holder);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).future().join();
        validate(val);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).future().join();
        validate(val);

        val++;
        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        int finalVal = val;
        Predicate<Request> pred = r -> {
            Command payload = r.getPayload();
            if (payload instanceof Replicate) {
                Put put = (Put) ((Replicate) payload).getData();
                return put.getKey() == finalVal;
            }
            return false;
        };
        toBob.client().block(pred);

        CompletableFuture<Void> fut = leaseholder.replicate(GRP_NAME, new Put(val, val)).future();
        assertFalse(fut.isDone());

        adjustClocks(Tracker.LEASE_DURATION / 2);

        toBob.client().stopBlock(pred);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    @Test
    public void testOutdatedReplication2() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(holder);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).future().join();
        validate(val);

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).future().join();
        validate(val);

        val++;
        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        int finalVal = val;
        Predicate<Request> pred = r -> {
            Command payload = r.getPayload();
            if (payload instanceof Replicate) {
                Put put = (Put) ((Replicate) payload).getData();
                return put.getKey() == finalVal;
            }
            return false;
        };
        toBob.client().block(pred);

        CompletableFuture<Void> fut = leaseholder.replicate(GRP_NAME, new Put(val, val)).future();
        assertFalse(fut.isDone());

        adjustClocks(Tracker.LEASE_DURATION / 2 + Tracker.MAX_CLOCK_SKEW);

        // Re-elect.
        assertTrue(tracker.assignLeaseholder(GRP_NAME, bob));
        waitLeaseholder(bob);

        toBob.client().stopBlock(pred);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    @Test
    public void testBrokenClocks() {
        // TODO
    }

    private void validate(int val) {
        assertEquals(val, top.getNode(alice).get(GRP_NAME, val, top.getNode(alice).clock().get()));
        assertEquals(val, top.getNode(bob).get(GRP_NAME, val, top.getNode(bob).clock().get()));
    }

    @Test
    public void testClosedGaps() {

    }
}
