package com.ascherbakoff.ai3.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Replicator.Inflight;
import com.ascherbakoff.ai3.cluster.Node;
import com.ascherbakoff.ai3.cluster.NodeId;
import com.ascherbakoff.ai3.cluster.Topology;
import com.ascherbakoff.ai3.util.BasicTest;
import java.lang.System.Logger.Level;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class ReplicatorTest extends BasicTest {
    public static final String GRP_NAME = "test";

    private static System.Logger LOGGER = System.getLogger(ReplicatorTest.class.getName());

    @Test
    public void testJoin() {
        Topology t = new Topology();

        Node alice = new Node(new NodeId("alice"), t);
        Node bob = new Node(new NodeId("bob"), t);

        t.regiser(alice);
        t.regiser(bob);

        assertEquals(2, t.getNodeMap().size());
    }

//    @Test
//    public void testSend() {
//        Topology t = new Topology();
//
//        Node alice = new Node(new NodeId("alice"), t);
//        Node bob = new Node(new NodeId("bob"), t);
//        bob.refresh(GRP_NAME, alice.clock().tick(), alice.id(), Collections.EMPTY_MAP);
//
//        t.regiser(alice);
//        t.regiser(bob);
//
//        Replicator aliceToBob = new Replicator(alice, bob.id(), GRP_NAME, t);
//        Response resp0 = aliceToBob.send(new Put(0, 0, nextId())).future().join();
//        assertNotNull(resp0);
//
//        alice.refresh(GRP_NAME, bob.clock().now(), bob.id(), Collections.EMPTY_MAP);
//
//        Replicator bobToAlice = new Replicator(bob, alice.id(), GRP_NAME, t);
//        Response resp1 = bobToAlice.send(new Put(1, 1, nextId())).future().join();
//        assertNotNull(resp1);
//
//        assertNotSame(resp0, resp1);
//    }




//
//    @Test
//    public void testLwmPropagationOutOfOrder2() throws InterruptedException {
//        Topology t = new Topology();
//
//        Node alice = new Node(new NodeId("alice"), t);
//        Node bob = new Node(new NodeId("bob"), t);
//
//        t.regiser(alice);
//        t.regiser(bob);
//
//        Replicator aliceToBob = new Replicator(alice, bob.id(), GRP_NAME, t);
//        aliceToBob.client().block(r -> true);
//
//        Timestamp t0 = aliceToBob.getLwm();
//
//        Inflight i1 = aliceToBob.send(new Put(0, 0, nextId()));
//        Inflight i2 = aliceToBob.send(new Put(1, 1, nextId()));
//
//        assertEquals(t0, bob.getLwm());
//
//        aliceToBob.client().stopBlock(r -> r.getTs().equals(i1.ts()));
//        i1.future().join();
//        assertEquals(t0, bob.getLwm());
//
//        Inflight i3 = aliceToBob.send(new Put(2, 2, nextId()));
//        aliceToBob.client().stopBlock(r -> r.getTs().equals(i3.ts()));
//        assertTrue(waitForCondition(() -> i3.isAcked(), 1000));
//        assertEquals(i1.ts(), bob.getLwm());
//
//        aliceToBob.client().stopBlock(r -> r.getTs().equals(i2.ts()));
//        assertTrue(waitForCondition(() -> i2.isAcked(), 1000));
//        assertEquals(i1.ts(), bob.getLwm());
//
//        i1.future().join();
//        i2.future().join();
//        i3.future().join();
//
//        assertEquals(0, aliceToBob.inflights());
//
//        aliceToBob.client().clearBlock();
//        aliceToBob.idleSync(r).join();
//
//        assertEquals(aliceToBob.getLwm(), bob.getLwm());
//    }

//    @Test
//    public void testSendConcurrent() throws InterruptedException {
//        Topology t = new Topology();
//
//        Node alice = new Node(new NodeId("alice"), t);
//        Node bob = new Node(new NodeId("bob"), t);
//
//        t.regiser(alice);
//        t.regiser(bob);
//
//        Replicator aliceToBob = new Replicator(alice, bob.id(), GRP_NAME, t);
//
//        Executor senderPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
//
//        int msgCnt = 100;
//
//        AtomicInteger x = new AtomicInteger();
//        CountDownLatch l = new CountDownLatch(msgCnt);
//
//        long ts = System.nanoTime();
//
//        while(msgCnt-- > 0) {
//            senderPool.execute(() -> {
//                int val = x.incrementAndGet();
//                aliceToBob.send(new Put(val, val, nextId())).future().thenAccept(r -> {
//                    l.countDown();
//                });
//            });
//        }
//
//        l.await();
//
//        assertEquals(0, l.getCount());
//
//        LOGGER.log(Level.INFO, "Finished sending messages, duration {0}ms", (System.nanoTime() - ts) / 1000 / 1000.);
//
//        aliceToBob.idleSync(r).join();
//
//        assertEquals(aliceToBob.getLwm(), bob.getLwm());
//    }


}
