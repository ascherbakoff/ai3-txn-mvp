package com.ascherbakoff.ai3.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class DeadlockTest {
    /**
     * Test if no deadlock from a cycle produced by single locker.
     */
    @Test
    public void testNoDeadlock() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);
        Lock lock2 = lockTable.getOrAddEntry(1);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock1.acquire(id1, LockMode.X);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == LockMode.X);

        Locker l2 = lock2.acquire(id1, LockMode.X);
        l2.join();
        assertTrue(l2.id == id1 && l2.mode == LockMode.X);

        Locker l3 = lock1.acquire(id1, LockMode.X);
        l3.join();
        assertTrue(l3.id == id1 && l3.mode == LockMode.X);
    }

    /**
     * Test deadlock.
     */
    @Test
    public void testDeadlockPreventWaitDie() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);
        Lock lock2 = lockTable.getOrAddEntry(1);

        UUID id1 = new UUID(0, 0); // older
        UUID id2 = new UUID(0, 1); // younger

        Locker l1_1 = lock1.acquire(id1, LockMode.X);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.X);

        Locker l2_1 = lock2.acquire(id2, LockMode.X);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.X);

        assertThrows(LockException.class, () -> lock1.acquire(id2, LockMode.X));
    }

    /**
     * Test deadlock on lock upgrade.
     */
    @Test
    public void testDeadlockUpgradeWaitDie() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);

        UUID id1 = new UUID(0, 0);
        UUID id2 = new UUID(0, 1);

        Locker l1_1 = lock1.acquire(id1, LockMode.S);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.S);

        Locker l2_1 = lock1.acquire(id2, LockMode.S);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.S);

        Locker l1_2 = lock1.acquire(id1, LockMode.X);
        assertFalse(l1_2.isDone());

        assertThrows(LockException.class, () -> lock1.acquire(id2, LockMode.X));
    }

    @Test
    public void testBadOrderWaitDie() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);

        UUID id2 = new UUID(0, 1);
        UUID id3 = new UUID(0, 2);
        UUID id1 = new UUID(0, 3);

        Locker l1 = lock1.acquire(id3, LockMode.X);
        l1.join();
        assertTrue(l1.id == id3 && l1.mode == LockMode.X);

        Locker l2 = lock1.acquire(id2, LockMode.X);
        assertFalse(l2.isDone());

        assertThrows(LockException.class, () -> lock1.acquire(id1, LockMode.X));
    }

    @Test
    public void testBadOrderCompatible1() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);

        UUID id1 = new UUID(0, 0);
        UUID id2 = new UUID(0, 1);
        UUID id3 = new UUID(0, 2);

        Locker l1 = lock1.acquire(id2, LockMode.S);
        l1.join();
        assertTrue(l1.id == id2 && l1.mode == LockMode.S);

        Locker l2 = lock1.acquire(id3, LockMode.S);
        l2.join();
        assertTrue(l2.id == id3 && l2.mode == LockMode.S);

        Locker l3 = lock1.acquire(id1, LockMode.S);
        l2.join();
        assertTrue(l3.id == id1 && l3.mode == LockMode.S);
    }

    @Test
    public void testBadOrderCompatible2() {
        LockTable lockTable = new LockTable(10, true, DeadlockPrevention.waitDie());

        Lock lock1 = lockTable.getOrAddEntry(0);

        UUID id1 = new UUID(0, 2);
        UUID id2 = new UUID(0, 1);
        UUID id3 = new UUID(0, 0);

        Locker l1 = lock1.acquire(id2, LockMode.S);
        l1.join();
        assertTrue(l1.id == id2 && l1.mode == LockMode.S);

        Locker l2 = lock1.acquire(id3, LockMode.S);
        l2.join();
        assertTrue(l2.id == id3 && l2.mode == LockMode.S);

        Locker l3 = lock1.acquire(id1, LockMode.S);
        l2.join();
        assertTrue(l3.id == id1 && l3.mode == LockMode.S);
    }
}
