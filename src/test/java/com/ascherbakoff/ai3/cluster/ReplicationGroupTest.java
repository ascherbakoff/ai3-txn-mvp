package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.cluster.Node.Result;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

public class ReplicationGroupTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(ReplicationGroupTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;

    private void createCluster() {
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top));

        tracker = new Tracker(top);

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);

        tracker.register(GRP_NAME, nodeIds);
        tracker.assignLeaseholder(GRP_NAME, alice);
        waitLeaseholder();
    }

    private void waitLeaseholder() {
        assertEquals(alice, tracker.getCurrentLeaseHolder(GRP_NAME));
        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> alice.equals(node.getLeaseHolder(GRP_NAME)), 1000));
        }
    }

    private void adjustClocks(long delta) {
        tracker.clock().adjust(delta);
        top.getNode(alice).clock().adjust(delta);
        top.getNode(bob).clock().adjust(delta);
    }

    @Test
    public void testBasicReplication() {
        createCluster();

        LOGGER.log(Level.INFO, "Tracker at {0}", tracker.clock().now());
        for (Node node : top.getNodeMap().values()) {
            LOGGER.log(Level.INFO, "Node {0} at {1}", node.id(), node.clock().now());
        }

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0, nextId())).future().join();

        assertEquals(0, top.getNode(alice).localGet(GRP_NAME, 0, nextId()).join());

        assertThrows(Exception.class, () -> top.getNode(bob).localGet(GRP_NAME, 0, nextId()).join());
    }

    @Test
    public void testClock() {
        createCluster();

        top.getNode(alice).replicate(GRP_NAME, new Put(0, 0, nextId())).future().join();

        Timestamp clock0 = top.getNode(alice).clock().now();
        Timestamp clock1 = top.getNode(alice).clock().now();

        assertEquals(clock0, clock1);
    }

    @Test
    public void testLwmPropagation() {
        createCluster();

        top.getNode(alice).replicate(GRP_NAME, new Put(0, 0, nextId())).future().join();

        Timestamp t = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t2 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getLwm();
        Timestamp t3 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();
        Timestamp t4 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t, t2);
        assertEquals(t2, t3);
        assertTrue(t3.compareTo(t4) > 0);

        top.getNode(alice).sync(GRP_NAME).join();

        Timestamp t5 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t3, t5);
    }

    @Test
    public void testLwmPropagationOutOfOrder() throws InterruptedException {
        createCluster();

        top.getNode(alice).createReplicator(GRP_NAME, bob);
        Replicator aliceToBob = top.getNode(alice).group(GRP_NAME).replicators.get(bob);
        aliceToBob.client().block(r -> true);

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();

        Result res0 = top.getNode(alice).replicate(GRP_NAME, new Put(0, 0, nextId()));
        Result res1 = top.getNode(alice).replicate(GRP_NAME, new Put(1, 1, nextId()));
        Result res2 = top.getNode(alice).replicate(GRP_NAME, new Put(2, 2, nextId()));

        aliceToBob.client().stopBlock(r -> r.getTs().equals(res2.getPending().get(bob).ts()));
        waitForCondition(() -> res2.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        aliceToBob.client().stopBlock(r -> r.getTs().equals(res1.getPending().get(bob).ts()));
        waitForCondition(() -> res1.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        aliceToBob.client().stopBlock(r -> r.getTs().equals(res0.getPending().get(bob).ts()));
        waitForCondition(() -> res0.getPending().get(bob).isAcked(), 1000);
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        res1.future().join();
        res1.future().join();
        res1.future().join();

        assertEquals(0, aliceToBob.inflights());

        Timestamp t = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        aliceToBob.client().clearBlock();
        top.getNode(alice).sync(GRP_NAME).join();

        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t, t3);
    }

    @Test
    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
        createCluster();

        top.getNode(alice).createReplicator(GRP_NAME, bob);
        Replicator aliceToBob = top.getNode(alice).group(GRP_NAME).replicators.get(bob);
        aliceToBob.client().block(r -> true);

        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getLwm();

        Result res1 = top.getNode(alice).replicate(GRP_NAME, new Put(0, 0, nextId()));
        Result res2 = top.getNode(alice).replicate(GRP_NAME, new Put(1, 1, nextId()));

        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        aliceToBob.client().stopBlock(r -> r.getTs().equals(res1.getPending().get(bob).ts()));
        res1.future().join();
        assertEquals(t0, top.getNode(bob).group(GRP_NAME).lwm);

        Result res3 = top.getNode(alice).replicate(GRP_NAME, new Put(3, 3, nextId()));
        Inflight i3 = res3.getPending().get(bob);
        aliceToBob.client().stopBlock(r -> r.getTs().equals(i3.ts()));
        assertTrue(waitForCondition(() -> i3.isAcked(), 1000));
        assertEquals(res1.getPending().get(bob).ts(), top.getNode(bob).group(GRP_NAME).lwm);

        aliceToBob.client().stopBlock(r -> r.getTs().equals(res2.getPending().get(bob).ts()));
        assertTrue(waitForCondition(() -> res2.getPending().get(bob).isAcked(), 1000));
        assertEquals(res1.getPending().get(bob).ts(), top.getNode(bob).group(GRP_NAME).lwm);

        res1.future().join();
        res2.future().join();
        res3.future().join();

        Timestamp t = top.getNode(alice).group(GRP_NAME).lwm;
        Timestamp t2 = top.getNode(bob).group(GRP_NAME).lwm;
        assertTrue(t.compareTo(t2) > 0);

        assertEquals(0, aliceToBob.inflights());

        aliceToBob.client().clearBlock();
        top.getNode(alice).sync(GRP_NAME).join();

        Timestamp t3 = top.getNode(bob).group(GRP_NAME).lwm;

        assertEquals(t, t3);
    }

    @Test
    public void testSendConcurrent() throws InterruptedException {
        createCluster();

        Executor senderPool = Executors.newFixedThreadPool(1);

        int msgCnt = 1000;

        AtomicInteger x = new AtomicInteger();
        AtomicInteger errCnt = new AtomicInteger();
        CountDownLatch l = new CountDownLatch(msgCnt);

        long ts = System.nanoTime();

        while(msgCnt-- > 0) {
            senderPool.execute(() -> {
                int val = x.incrementAndGet();
                top.getNode(alice).replicate(GRP_NAME, new Put(val, val, nextId())).future().exceptionally(new Function<Throwable, Void>() {
                    @Override
                    public Void apply(Throwable err) {
                        errCnt.incrementAndGet();
                        LOGGER.log(Level.ERROR, "Failed to replicate", err);
                        return null;
                    }
                }).thenAccept(r -> l.countDown());
            });
        }

        l.await();

        assertEquals(0, l.getCount());
        assertEquals(0, errCnt.get());

        LOGGER.log(Level.INFO, "Finished sending messages, duration {0}ms", (System.nanoTime() - ts) / 1000 / 1000.);

        top.getNode(alice).sync(GRP_NAME).join();

        assertEquals(top.getNode(alice).group(GRP_NAME).lwm, top.getNode(bob).group(GRP_NAME).lwm);
    }
}
