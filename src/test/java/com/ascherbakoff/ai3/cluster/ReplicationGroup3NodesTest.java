package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class ReplicationGroup3NodesTest extends BasicTest {
    public static final String GRP_NAME = "testGrp";

    private static System.Logger LOGGER = System.getLogger(ReplicationGroup3NodesTest.class.getName());

    Topology top;
    Tracker tracker;
    NodeId alice;
    NodeId bob;
    NodeId charlie;
    NodeId leader;

    private void createCluster() {
        top = new Topology();

        alice = new NodeId("alice");
        top.regiser(new Node(alice, top, clock));

        bob = new NodeId("bob");
        top.regiser(new Node(bob, top, clock));

        charlie = new NodeId("charlie");
        top.regiser(new Node(charlie, top, clock));

        List<NodeId> nodeIds = new ArrayList<>();
        nodeIds.add(alice);
        nodeIds.add(bob);
        nodeIds.add(charlie);

        leader = alice;

        tracker = new Tracker(top, clock);
        tracker.register(GRP_NAME, nodeIds);
        tracker.assignLeaseholder(GRP_NAME, leader);

        waitLeaseholder(leader);
    }

    private void waitLeaseholder(NodeId nodeId) {
        assertEquals(nodeId, tracker.getLeaseHolder(GRP_NAME));

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> nodeId.equals(node.getLeaseHolder(GRP_NAME)), 1_000));
        }
    }

    @Test
    public void testBasicReplication() {
        createCluster();

        Node leaseholder = top.getNode(leader);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();

        for (Node node : top.getNodeMap().values()) {
            assertTrue(waitForCondition(() -> {
                Integer val = node.localGet(GRP_NAME, 0, node.clock().get());
                return val != null && 0 == val.intValue();
            }, 1_000));
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

        // Wait for replication.
        assertTrue(waitForCondition(() -> {
            top.getNode(leader).sync(GRP_NAME).join();

            for (Node node : top.getNodeMap().values()) {
                Timestamp lwm = top.getNode(leader).group(GRP_NAME).replicators.get(node.id()).getLwm();
                if (!lwm.equals(node.group(GRP_NAME).lwm)) {
                    return false;
                }
            }

            return true;
        }, 1_000));

        for (int i = 0; i < msgCnt; i++) {
            for (Node node : top.getNodeMap().values()) {
                assertEquals(i, node.localGet(GRP_NAME, i, node.group(GRP_NAME).lwm));
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
        Timestamp oldLease = tracker.getLease(GRP_NAME);
        assertNotNull(oldLease);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.

        leaseholder.sync(GRP_NAME).join();

        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getPayload() instanceof Replicate);

        val++;
        CompletableFuture<Void> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
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
        assertTrue(inflight.error());

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
}
