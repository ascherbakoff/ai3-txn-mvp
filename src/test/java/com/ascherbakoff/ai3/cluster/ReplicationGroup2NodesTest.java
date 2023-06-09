package com.ascherbakoff.ai3.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Put;
import com.ascherbakoff.ai3.replication.Replicate;
import com.ascherbakoff.ai3.replication.Replicator;
import com.ascherbakoff.ai3.replication.Inflight;
import com.ascherbakoff.ai3.replication.Request;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.Test;

/**
 * The leasholder is a standalone node.
 */
public class ReplicationGroup2NodesTest extends BasicReplicationTest {
    int val = 0;

    private static System.Logger LOGGER = System.getLogger(ReplicationGroup2NodesTest.class.getName());

    @Test
    public void testBasicReplication() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(alice);
        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
        waitReplication();

        Group grp0 = top.getNode(alice).group(GRP_NAME);
        Group grp1 = top.getNode(bob).group(GRP_NAME);

        assertEquals(grp0, grp1);

        leaseholder.replicate(GRP_NAME, new Put(1, 1)).join();
        waitReplication();

        grp0 = top.getNode(alice).group(GRP_NAME);
        grp1 = top.getNode(bob).group(GRP_NAME);

        assertEquals(grp0, grp1);
    }


    @Test
    public void testIdlePropagation() throws InterruptedException, ExecutionException {
        createCluster();

        Node leaseholder = top.getNode(alice);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();
        adjustClocks(20);

        Timestamp ts = leaseholder.sync(GRP_NAME).get();
        waitReplication();

        Group grp0 = top.getNode(alice).group(GRP_NAME);
        Group grp1 = top.getNode(bob).group(GRP_NAME);

        assertEquals(ts, grp0.getRepTs());
        assertEquals(ts, grp1.getRepTs());

        assertEquals(1, grp0.getRepCntr());
        assertEquals(1, grp1.getRepCntr());
    }

    @Test
    public void testReorder() throws InterruptedException {
        createCluster();

        Node leaseholder = top.getNode(alice);
        int val = 0;
        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        Group grp0 = top.getNode(alice).group(GRP_NAME);
        Group grp1 = top.getNode(bob).group(GRP_NAME);

        Timestamp ts0_0 = grp0.getRepTs();

        Timestamp ts1_0 = grp1.getRepTs();
        long cntr1_0 = grp1.getRepCntr();

        top.getNode(alice).getReplicator(GRP_NAME, bob);
        Replicator toBob = top.getNode(alice).group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        val++;
        CompletableFuture<Timestamp> res0 = top.getNode(alice).replicate(GRP_NAME, new Put(val, val));
        val++;
        CompletableFuture<Timestamp> res1 = top.getNode(alice).replicate(GRP_NAME, new Put(val, val));

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 2, 1000));
        ArrayList<Request> blocked = toBob.client().blocked();

        toBob.client().unblock(r -> r.getTs().equals(blocked.get(1).getTs()));
        assertTrue(waitForCondition(() -> toBob.inflight(blocked.get(1)).ioFuture().isDone(), 1000));

        Timestamp repTs = grp0.getRepTs();
        long cntr0 = grp0.getRepCntr();
        assertNotEquals(ts0_0, repTs);

        // Rep cntr should not change.
        assertEquals(ts1_0, grp1.getRepTs());
        assertEquals(cntr1_0, grp1.getRepCntr());

        assertEquals(ts1_0, grp0.getSafeTs());
        assertEquals(cntr1_0, grp0.getSafeCntr());

        Inflight inflight = toBob.inflight(blocked.get(0));
        toBob.client().unblock(r -> r.getTs().equals(blocked.get(0).getTs()));
        assertTrue(waitForCondition(() -> {
            return inflight.ioFuture().isDone();
        }, 1000));

        assertEquals(repTs, grp1.getRepTs());
        assertEquals(cntr0, grp1.getRepCntr());

        assertEquals(grp0, grp1);

        res0.join();
        res1.join();
    }


    @Test
    public void testSendConcurrent() throws InterruptedException {
        createCluster();

        Executor senderPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        int msgCntr = 1000;
        int cntr = msgCntr;

        AtomicInteger gen = new AtomicInteger();
        AtomicInteger errCnt = new AtomicInteger();
        CountDownLatch l = new CountDownLatch(msgCntr);

        long ts = System.nanoTime();

        Node leader = top.getNode(alice);

        while(cntr-- > 0) {
            senderPool.execute(() -> {
                int val = gen.incrementAndGet();
                leader.replicate(GRP_NAME, new Put(val, val)).exceptionally(err -> {
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

        waitReplication();

        TreeMap<Timestamp, Replicate> snapIdx = leader.group(GRP_NAME).snapIdx;
        TreeMap<Timestamp, Replicate> snapIdx2 = top.getNode(bob).group(GRP_NAME).snapIdx;

        assertEquals(msgCntr, snapIdx.size());
        assertEquals(snapIdx, snapIdx2);;
    }
//
//    @Test
//    public void testLeaseholderFailure() {
//        createCluster();
//    }
//

    /**
     * Tests if messages from invalid leaseholder are ignored.
     */
    @Test
    public void testOutdatedReplication() {
        createCluster();

        Node leader = top.getNode(alice);
        int val = 0;
        leader.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leader.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        val++;
        Replicator toBob = leader.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        CompletableFuture<Timestamp> fut = leader.replicate(GRP_NAME, new Put(val, val));
        assertFalse(fut.isDone());

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));

        adjustClocks(Tracker.LEASE_DURATION / 2);

        toBob.client().unblock(r -> true);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    @Test
    public void testOutdatedReplication2() throws InterruptedException {
        createCluster();

        Node leader = top.getNode(alice);
        int val = 0;
        leader.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        val++;
        leader.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        val++;
        Replicator toBob = leader.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(r -> true);

        CompletableFuture<Timestamp> fut = leader.replicate(GRP_NAME, new Put(val, val));
        assertFalse(fut.isDone());

        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));

        adjustClocks(Tracker.LEASE_DURATION / 2 + Tracker.MAX_CLOCK_SKEW);

        // Re-elect.
        Timestamp ts = tracker.assignLeader(GRP_NAME, bob, nodeIds).join();
        waitLeader(ts, bob, tracker, top, GRP_NAME);

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

        Node leader = top.getNode(alice);
        int val = 0;
        leader.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.

        Replicator toBob = leader.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getTs().physical() < Tracker.LEASE_DURATION / 2);

        val++;
        CompletableFuture<Timestamp> fut = leader.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        assertFalse(fut.isDone());

        adjustClocks(Tracker.LEASE_DURATION / 2);

        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, nodeIds).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        adjustClocks(Tracker.LEASE_DURATION / 2 + Tracker.MAX_CLOCK_SKEW);

        toBob.client().unblock(r -> true);

        assertThrows(CompletionException.class, () -> fut.join());
    }

    /**
     * 1. One of replication messages is delayed infinitely.
     * <p>Expected result: operation is failed after some timeout, closing the gap on affected replicator.
     * Note: not closing gaps leads to prevention of safeTime propagation.
     */
    @Test
    public void testClosedGaps() throws Exception {

    }

    /**
     * 1. One of replication messages is delayed infinitely.
     * <p>Expected result: operation is failed after some timeout, closing the gap on affected replicator.
     * Note: not closing gaps leads to prevention of safeTime propagation.
     */
//    @Test
//    public void testClosedGaps() throws Exception {
//        createCluster();
//
//        Node leaseholder = top.getNode(leader);
//        int val = 0;
//        leaseholder.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.
//
//        leaseholder.sync(GRP_NAME).join();
//
//        Replicator toBob = leaseholder.group(GRP_NAME).replicators.get(bob);
//        toBob.client().block(request -> request.getPayload() instanceof Replicate);
//
//        val++;
//        CompletableFuture<Timestamp> fut = leaseholder.replicate(GRP_NAME, new Put(val, val));
//        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
//        assertFalse(fut.isDone());
//
//        Thread.sleep(200); // Give time to finish for alice->alice replication.
//
//        // LWM shoudn't propagate for alice->bob until infligh is not completed.
//        leaseholder.sync(GRP_NAME).join();
//
//        Timestamp t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getSafeCntr();
//        Timestamp t1 = top.getNode(alice).group(GRP_NAME).repTs;
//        assertEquals(t0, t1);
//        Timestamp t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getSafeCntr();
//        Timestamp t3 = top.getNode(bob).group(GRP_NAME).repTs;
//        assertEquals(t2, t3);
//        assertTrue(t0.compareTo(t2) > 0);
//        assertTrue(t1.compareTo(t3) > 0);
//
//        assertThrows(CompletionException.class, () -> fut.join());
//
//        leaseholder.sync(GRP_NAME).join();
//
//        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getSafeCntr();
//        t1 = top.getNode(alice).group(GRP_NAME).repTs;
//        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getSafeCntr();
//        t3 = top.getNode(bob).group(GRP_NAME).repTs;
//        assertEquals(t0, t1);
//        assertEquals(t1, t2);
//        assertEquals(t2, t3);
//
//        toBob.client().clearBlock();
//
//        val++;
//        CompletableFuture<Timestamp> fut2 = leaseholder.replicate(GRP_NAME, new Put(val, val));
//        fut2.join();
//
//        leaseholder.sync(GRP_NAME).join();
//
//        t0 = top.getNode(alice).group(GRP_NAME).replicators.get(alice).getSafeCntr();
//        t1 = top.getNode(alice).group(GRP_NAME).repTs;
//        t2 = top.getNode(alice).group(GRP_NAME).replicators.get(bob).getSafeCntr();
//        t3 = top.getNode(bob).group(GRP_NAME).repTs;
//        assertEquals(t0, t1);
//        assertEquals(t1, t2);
//        assertEquals(t2, t3);
//    }
//
//    /**
//     * 1. One of replication messages is delayed infinitely.
//     * <p>2. Many replication commands are issued, causing inflights overflow.
//     * <p>Expected result: on overflow the replicator wents to error state, preventing any replication activity.
//     * The correposnding node must be restarted to re-create replicator and perform catch-up.
//     * Note: the same behavior must be applied on replication command uncaught exception.
//     */
//    @Test
//    public void testReplicatorErrorOnOverflow() {
//        fail();
//    }
//
//    @Test
//    public void testReplicatorErrorOnTimeout() {
//        fail();
//    }
//
//    @Test
//    public void testReplicatorErrorOnBadResponse() {
//        fail();
//    }
//
//    @Test
//    public void testGroupInErrorStateCannotBecomeLeader() {
//        fail();
//    }
//
//    /**
//     * Tests if a message containing illegal ts value (in the future) is ignored.
//     */
//    @Test
//    public void testBrokenClocks() {
//        fail();
//    }
//
    /**
     * Tests if a lease can always be refreshed if current holder is available.
     */
    @Test
    public void testAssignNoMajority() throws InterruptedException {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        assertNotNull(top.getNodeMap().remove(bob));

        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, nodeIds).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        validate(alice);

        Node leader = top.getNode(alice);

        CompletableFuture<Timestamp> fut = leader.replicate(GRP_NAME, new Put(0, 0));
        assertThrows(CompletionException.class, () -> fut.join(), "Replication must fail");
    }

    /**
     * Tests if a leader can be reassigned to any alive node if replication factor matches group size. Currently this is true for 2 node cluster. Need replication factor support for larger clusters.
     */
    @Test
    public void testReassignNoMajority() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        assertNotNull(top.getNodeMap().remove(bob));

        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, nodeIds).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        validate(alice);
    }

    @Test
    public void testReassignEmpty() {
        createCluster();

        adjustClocks(Tracker.LEASE_DURATION + Tracker.MAX_CLOCK_SKEW);

        assertNotNull(top.getNodeMap().remove(alice));
        assertNotNull(top.getNodeMap().remove(bob));

        assertThrows(CompletionException.class, () -> tracker.assignLeader(GRP_NAME, alice, nodeIds).join(), "Can't assign on empty group");
    }

    /**
     * Tests if a failed replication disables a replicator.
     *
     */
    @Test
    public void testBrokenPipe() {
        createCluster();

        Node leader = top.getNode(alice);
        leader.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.
        waitReplication();

        Group grp0 = top.getNode(alice).group(GRP_NAME);
        Group grp1 = top.getNode(bob).group(GRP_NAME);

        long cntr0 = grp0.getRepCntr();
        Timestamp ts0 = grp0.getRepTs();

        Replicator toBob = leader.group(GRP_NAME).replicators.get(bob);
        toBob.client().block(request -> request.getPayload() instanceof Replicate);

        val++;
        CompletableFuture<Timestamp> fut = leader.replicate(GRP_NAME, new Put(val, val));
        assertTrue(waitForCondition(() -> toBob.client().blocked().size() == 1, 1_000));
        assertThrows(CompletionException.class, () -> fut.join());

        assertEquals(cntr0, grp0.getSafeCntr());
        assertEquals(ts0, grp0.getSafeTs());

        assertEquals(cntr0, grp1.getRepCntr());
        assertEquals(ts0, grp1.getRepTs());

        assertTrue(grp0.replicators.get(bob).broken());

        toBob.client().clearBlock();

        val++;
        CompletableFuture<Timestamp> fut2 = leader.replicate(GRP_NAME, new Put(val, val));
        assertThrows(CompletionException.class, () -> fut2.join());
    }

    /**
     * Tests a dead node re-joins and catches up with the group.
     */
    @Test
    public void testCatchUpWithRefresh() {
        createCluster();

        Node leader = top.getNode(alice);
        leader.replicate(GRP_NAME, new Put(val, val)).join(); // Init replicators.
        waitReplication();

        adjustClocks(Tracker.LEASE_DURATION / 2);

        Node bobNode = top.getNode(bob);

        assertNotNull(top.getNodeMap().remove(bob));

        Timestamp ts = tracker.assignLeader(GRP_NAME, alice, top.getNodeMap().keySet()).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        assertTrue(leader.group(GRP_NAME).replicators.get(bob) == null); // Replicator should be removed on node removal.

        val++;
        leader.replicate(GRP_NAME, new Put(val, val)).join();
        waitReplication();

        // Restore node in topology.
        top.regiser(bobNode);

        ts = tracker.assignLeader(GRP_NAME, alice, top.getNodeMap().keySet()).join();
        waitLeader(ts, alice, tracker, top, GRP_NAME);

        val++;
        leader.replicate(GRP_NAME, new Put(val, val)).join();

        waitReplication();

        assertTrue(waitForCondition(() -> {
            return leader.group(GRP_NAME).getMembers().size() == 2;
        }, 1_000));

        System.out.println();
    }
//
//    /**
//     * Tests if a Put operation can't be committed ahead of pending Configure.
//     */
//    @Test
//    public void testConfigurePutReorderNotPossible() {
//
//    }
//
//    /**
//     * Tests if a Put operation is rehected if an older Configure failes.
//     */
//    @Test
//    public void testConfigureFailPutFail() {
//
//    }
//
//    /**
//     * Tests replication group size upscale. New node is expected to catch up with operational node.
//     */
//    @Test
//    public void testReconfigurationUpscale() throws InterruptedException {
//        createCluster();
//
//        Node leaseholder = top.getNode(alice);
//        int val = 0;
//        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
//
//        Node node = new Node(charlie, top, clock);
//        top.addNode(node);
//
//        Set<NodeId> newMembers = top.getNodeMap().keySet();
//
//        // Make sure new nodes know about leaseholder.
//        Timestamp ts = tracker.assignLeader(GRP_NAME, leader, newMembers).join();
//        waitLeader(ts, leader, tracker, top, GRP_NAME);
//        validate(leader, charlie);
//        assertTrue(waitForCondition(() -> State.CATCHINGUP == top.getNode(charlie).group(GRP_NAME).state, 1000));
//
//        // TODO validate lease
//        assertEquals(Timestamp.min(), top.getNode(charlie).group(GRP_NAME).repTs, "New node is expected to be empty");
//
//        val++;
//        leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
//
//        assertEquals(Timestamp.min(), top.getNode(charlie).group(GRP_NAME).repTs, "LWM should not be propagated");
//        assertTrue(waitForCondition(() -> top.getNode(alice).group(GRP_NAME).repTs.equals(top.getNode(charlie).group(GRP_NAME).tmpLwm), 1000));
//
//        leaseholder.sync(GRP_NAME).join();
//
//        assertEquals(Timestamp.min(), top.getNode(charlie).group(GRP_NAME).repTs, "LWM should not be propagated");
//        assertTrue(waitForCondition(() -> top.getNode(alice).group(GRP_NAME).repTs.equals(top.getNode(charlie).group(GRP_NAME).tmpLwm), 1000));
//
//        val++;
//        Timestamp ts2 = leaseholder.replicate(GRP_NAME, new Put(val, val)).join();
//        LOGGER.log(Level.INFO, "Replicated with ts={0}", ts2);
//
//        assertTrue(ts2.compareTo(top.getNode(leader).group(GRP_NAME).getActivationTs()) > 0);
//
//        top.getNode(charlie).catchUp(GRP_NAME).join();
//        assertEquals(Timestamp.min(), top.getNode(charlie).group(GRP_NAME).tmpLwm);
//        validate(leader);
//
//        validate(val);
//    }
//
//    @Test
//    public void testReconfigurationDownscale() throws InterruptedException {
//        createCluster();
//
//        Node leaseholder = top.getNode(alice);
//        leaseholder.replicate(GRP_NAME, new Put(0, 0)).join();
//
//        Set<NodeId> newMembers = Set.of(alice);
//
//        // Propagate new configuration in lease refresh.
//        Timestamp ts = tracker.assignLeader(GRP_NAME, leader, newMembers).join();
//
//        for (NodeId nodeId : newMembers) {
//            Group locGroup = top.getNode(nodeId).group(GRP_NAME);
//            assertTrue(waitForCondition(() -> newMembers.equals(locGroup.getMembers()), 1000), nodeId.toString());
//        }
//
//        leaseholder.replicate(GRP_NAME, new Put(1, 1)).join();
//
//        assertTrue(top.getNode(alice).group(GRP_NAME).repTs.compareTo(top.getNode(bob).group(GRP_NAME).repTs) > 0, "Out of sync expected");
//
//        leaseholder.sync(GRP_NAME).join();
//
//        assertTrue(top.getNode(alice).group(GRP_NAME).repTs.compareTo(top.getNode(bob).group(GRP_NAME).repTs) > 0, "Out of sync expected");
//    }
//
//    @Test
//    public void testLeaderChangeDuringReplication() {
//        createCluster();
//
//        Node leaseholder = top.getNode(alice);
//        int v = 0;
//        leaseholder.replicate(GRP_NAME, new Put(v, v)).join();
//
//        for (Node value : top.getNodeMap().values()) {
//            Replicator rep = top.getNode(alice).group(GRP_NAME).replicators.get(value.id());
//            rep.client().block(r -> r.getPayload() instanceof Finish);
//        }
//
//        v++;
//        leaseholder.replicate(GRP_NAME, new Put(v, v));
//
//        for (Node value : top.getNodeMap().values()) {
//            Replicator rep = top.getNode(alice).group(GRP_NAME).replicators.get(value.id());
//            assertTrue(waitForCondition(() -> rep.client().blocked().size() == 1, 1_000));
//        }
//
//        for (Node value : top.getNodeMap().values()) {
//            Replicator rep = top.getNode(alice).group(GRP_NAME).replicators.get(value.id());
//            rep.client().clearBlock();
//        }
//
//        v++;
//        leaseholder.replicate(GRP_NAME, new Put(v, v)).join();
//
//        System.out.println();
//    }
//
//    @Test
//    public void testLeaderFailedDuringLeaseRefresh() {
//        fail();
//    }
//
//    /**
//     * Tests if uncommitted entries are skipped on snapshot.
//     */
//    @Test
//    public void testCatchupWithUncommitted() {
//        fail();
//    }
//
    /**
     * Tests if a non leader is attempting to replicate.
     */
    @Test
    public void testNonLeaderReplication() {
        createCluster();

        Node leader = top.getNode(alice);
        leader.replicate(GRP_NAME, new Put(0, 0)).join();

        assertThrows(CompletionException.class, () -> top.getNode(bob).replicate(GRP_NAME, new Put(0, 0)).join());
    }

//    private void validate(int val) {
//        for (Node value : top.getNodeMap().values()) {
//            assertEquals(val, top.getNode(alice).localGet(GRP_NAME, val, top.getNode(value.id()).group(GRP_NAME).repTs).join());
//            assertEquals(top.getNode(alice).group(GRP_NAME).replicators.get(value.id()).getSafeCntr(), top.getNode(value.id()).group(GRP_NAME).repTs);
//        }
//    }
//
//    private void validateAtTimestamp(int val, Timestamp ts) {
//        for (Node value : top.getNodeMap().values()) {
//            assertEquals(val, value.localGet(GRP_NAME, val, ts).join());
//        }
//    }
}
