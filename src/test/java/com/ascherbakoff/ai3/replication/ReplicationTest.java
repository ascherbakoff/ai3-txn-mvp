package com.ascherbakoff.ai3.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.tracker.Node;
import com.ascherbakoff.ai3.tracker.NodeId;
import com.ascherbakoff.ai3.tracker.Topology;
import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class ReplicationTest extends BasicTest {
    private static System.Logger LOGGER = System.getLogger(ReplicationTest.class.getName());

    @Test
    public void testJoin() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        assertEquals(2, t.getNodeMap().size());
    }

    @Test
    public void testSend() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        Response resp0 = aliceToBob.send(new Put(0, 0)).future().join();
        assertNotNull(resp0);

        Replicator bobToAlice = new Replicator(alice.id(), t);
        Response resp1 = bobToAlice.send(new Put(1, 1)).future().join();
        assertNotNull(resp1);

        assertNotSame(resp0, resp1);
    }

    @Test
    public void testLwmPropagation() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        assertNotNull(aliceToBob.send(new Put(0, 0)).future().join());

        Timestamp t0 = aliceToBob.getLwm();

        assertNotNull(aliceToBob.send(new Put(1, 1)).future().join());

        Timestamp t1 = bob.getLwm();
        Timestamp t2 = aliceToBob.getLwm();

        assertEquals(t0, t1);

        aliceToBob.idleSync().join();

        Timestamp t3 = bob.getLwm();

        assertEquals(t2, t3);
    }

    @Test
    public void testLwmPropagationOutOfOrder() throws InterruptedException {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        aliceToBob.block(r -> true);

        Timestamp t0 = aliceToBob.getLwm();

        Inflight i1 = aliceToBob.send(new Put(0, 0));
        Inflight i2 = aliceToBob.send(new Put(1, 1));
        Inflight i3 = aliceToBob.send(new Put(2, 2));

        assertEquals(t0, bob.getLwm());

        aliceToBob.stopBlock(r -> r.getTs().equals(i3.ts()));
        waitForCondition(() -> i3.isAcked(), 1000);
        assertEquals(t0, bob.getLwm());

        aliceToBob.stopBlock(r -> r.getTs().equals(i2.ts()));
        waitForCondition(() -> i2.isAcked(), 1000);
        assertEquals(t0, bob.getLwm());

        aliceToBob.stopBlock(r -> r.getTs().equals(i1.ts()));
        waitForCondition(() -> i1.isAcked(), 1000);
        assertEquals(t0, bob.getLwm());

        i1.future().join();
        i2.future().join();
        i3.future().join();

        assertEquals(0, aliceToBob.inflights());

        aliceToBob.idleSync().join();

        assertEquals(aliceToBob.getLwm(), bob.getLwm());
    }

    @Test
    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);
        aliceToBob.block(r -> true);

        Timestamp t0 = aliceToBob.getLwm();

        Inflight i1 = aliceToBob.send(new Put(0, 0));
        Inflight i2 = aliceToBob.send(new Put(1, 1));

        assertEquals(t0, bob.getLwm());

        aliceToBob.stopBlock(r -> r.getTs().equals(i1.ts()));
        i1.future().join();
        assertEquals(t0, bob.getLwm());

        Inflight i3 = aliceToBob.send(new Put(2, 2));
        aliceToBob.stopBlock(r -> r.getTs().equals(i3.ts()));
        assertTrue(waitForCondition(() -> i3.isAcked(), 1000));
        assertEquals(i1.ts(), bob.getLwm());

        aliceToBob.stopBlock(r -> r.getTs().equals(i2.ts()));
        assertTrue(waitForCondition(() -> i2.isAcked(), 1000));
        assertEquals(i1.ts(), bob.getLwm());

        i1.future().join();
        i2.future().join();
        i3.future().join();

        assertEquals(0, aliceToBob.inflights());

        aliceToBob.idleSync().join();

        assertEquals(aliceToBob.getLwm(), bob.getLwm());
    }

    @Test
    public void testSendConcurrent() throws InterruptedException {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"));
        Node bob = new Node(new NodeId("bob"));

        t.regiser(alice);
        t.regiser(bob);

        Replicator aliceToBob = new Replicator(bob.id(), t);

        Executor senderPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

        int msgCnt = 100;

        AtomicInteger x = new AtomicInteger();
        CountDownLatch l = new CountDownLatch(msgCnt);

        long ts = System.nanoTime();

        while(msgCnt-- > 0) {
            senderPool.execute(() -> {
                int val = x.incrementAndGet();
                aliceToBob.send(new Put(val, val)).future().thenAccept(r -> {
                    l.countDown();
                });
            });
        }

        l.await();

        assertEquals(0, l.getCount());

        LOGGER.log(Level.INFO, "Finished sending messages, duration {0}ms", (System.nanoTime() - ts) / 1000 / 1000.);

        aliceToBob.idleSync().join();

        assertEquals(aliceToBob.getLwm(), bob.getLwm());
    }
}
