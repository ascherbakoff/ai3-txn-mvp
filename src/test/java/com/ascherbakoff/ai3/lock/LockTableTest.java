package com.ascherbakoff.ai3.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.EnumSet;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 *
 */
public class LockTableTest {
    private LockTable lockTable = new LockTable(10, true, DeadlockPrevention.none());

    /**
     * Tests basic lock/unlock for all lock modes.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testLockUnlock(LockMode lockMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, lockMode);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == lockMode);

        lock.release(l1);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests that incompatible locks can't be acquired in the same time by different lockers.
     *
     * @param lockMode
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testLockLockUnlockUnlockIncompatible(LockMode lockMode) {
        if (lockMode.compatible(lockMode))
            return;

        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, lockMode);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == lockMode);

        Locker l2 = lock.acquire(id2, lockMode);
        assertFalse(l2.isDone());

        assertTrue(lock.owners.size() == 1);
        assertFalse(lock.waiters.isEmpty());

        lock.release(l1);
        l2.join();
        assertTrue(l2.id == id2 && l2.mode == lockMode);
        assertTrue(l2.isDone());

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l2);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests that compatible locks can be acquired in the same time by different lockers.
     *
     * @param lockMode Lock mode.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testLockLockUnlockUnlockCompatible(LockMode lockMode) {
        if (!lockMode.compatible(lockMode))
            return;

        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, lockMode);
        l1.join();

        assertTrue(l1.id == id1 && l1.mode == lockMode);

        Locker l2 = lock.acquire(id2, lockMode);
        l2.join();

        assertTrue(lock.owners.size() == 2);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l1);

        assertTrue(lock.owners.size() == 1);

        lock.release(l2);
        assertTrue(lock.owners.isEmpty());
    }

    /**
     * Tests a reenter with all lock modes.
     *
     * @param lockMode Lock mode.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testReenter(LockMode lockMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.X);
        assertNull(l1.join());
        assertTrue(l1.id == id1 && l1.mode == LockMode.X);

        Locker l2 = lock.acquire(id1, lockMode);
        assertEquals(LockMode.X, l2.join());
        assertTrue(l2.id == id1 && l2.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l1);
        assertTrue(lock.owners.isEmpty());
    }

    /**
     * Tests direct upgrade to the supremum for the same locker.
     *
     * @param fromMode Lock mode.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testDirectUpgradeSupremum(LockMode fromMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        EnumSet<LockMode> range = EnumSet.range(fromMode, LockMode.X);

        for (LockMode toMode : range) {
            Locker l1 = lock.acquire(id1, fromMode);
            l1.join();
            assertTrue(l1.id == id1 && l1.mode == fromMode);

            Locker l2 = lock.acquire(id1, toMode);
            l2.join();
            assertTrue(l2.id == id1 && l2.mode == LockTable.supremum(fromMode, toMode));

            assertTrue(lock.owners.size() == 1);
            assertTrue(lock.waiters.isEmpty());

            lock.release(l2);

            assertTrue(lock.owners.isEmpty());
            assertTrue(lock.waiters.isEmpty());
        }
    }

    /**
     * Tests direct upgrade IS_lock, IX_lock, X_lock.
     */
    @Test
    public void testDirectUpgradeMulti() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.IS);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == LockMode.IS);

        Locker l2 = lock.acquire(id1, LockMode.IX);
        l2.join();
        assertTrue(l2.id == id1 && l2.mode == LockMode.IX);

        Locker l3 = lock.acquire(id1, LockMode.X);
        l3.join();
        assertTrue(l3.id == id1 && l3.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l3); // We hold X lock

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests direct upgrade IS_lock(1), IS_lock(2), IX_lock(1), IS_unlock(2), X_lock(1).
     */
    @Test
    public void testDirectUpgradeMulti2() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.IS);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.IS);

        Locker l2_1 = lock.acquire(id2, LockMode.IS);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.IS);

        Locker l1_2 = lock.acquire(id1, LockMode.IX);
        l1_2.join();
        assertTrue(l1_2.id == id1 && l1_2.mode == LockMode.IX);

        Locker l1_3 = lock.acquire(id1, LockMode.X);
        assertFalse(l1_3.isDone());

        lock.release(l2_1);

        l1_3.join();
        assertTrue(l1_3.id == id1 && l1_3.mode == LockMode.X);

        lock.release(l1_3);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    @Test
    public void testDirectUpgradeMulti3() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.IX);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.IX);

        Locker l2_1 = lock.acquire(id2, LockMode.IS);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.IS);

        Locker l1_2 = lock.acquire(id1, LockMode.S);
        l1_2.join();
        assertTrue(l1_2.id == id1 && l1_2.mode == LockMode.SIX);

        Locker l1_3 = lock.acquire(id1, LockMode.X);
        assertFalse(l1_3.isDone());

        lock.release(l2_1);

        l1_3.join();
        assertTrue(l1_3.id == id1 && l1_3.mode == LockMode.X);

        lock.release(l1_3);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests if a direct upgrade is blocked by conflicting lock.
     *
     * The lock sequence is S_lock(1), S_lock(2), X_lock(2), S_unlock(1).
     */
    @Test
    public void testBlockedUpgrade() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.S);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == LockMode.S);

        Locker l2_1 = lock.acquire(id2, LockMode.S);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.S);

        Locker l2_2 = lock.acquire(id2, LockMode.X);
        assertFalse(l2_2.isDone());

        lock.release(l1);
        l2_2.join();

        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests direct upgrade of compatible locks.
     *
     * The lock sequence is IS_lock(1), IS_lock(2), IX_lock(2), IX_lock(1).
     */
    @Test
    public void testDirectUpgradeCompatible() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.IS);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.IS);

        Locker l2_1 = lock.acquire(id2, LockMode.IS);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l1_1.mode == LockMode.IS);

        Locker l2_2 = lock.acquire(id2, LockMode.IX);
        l2_2.join();
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.IX);

        Locker l1_2 = lock.acquire(id1, LockMode.IX);
        l1_2.join();
        assertTrue(l1_2.id == id1 && l1_2.mode == LockMode.IX);

        assertTrue(lock.owners.size() == 2);
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests if a direct upgrade is blocked by conflicting lock.
     *
     * The lock sequence is IS_lock(1), IS_lock(2), IX_lock(2), X_lock(2), IS_unlock(1).
     */
    @Test
    public void testBlockedUpgradeCompatible() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.IS);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.IS);

        Locker l2_1 = lock.acquire(id2, LockMode.IS);
        l2_1.join();
        assertTrue(l2_1.id == id2 && l1_1.mode == LockMode.IS);

        Locker l2_2 = lock.acquire(id2, LockMode.IX);
        l2_2.join();
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.IX);

        Locker l2_3 = lock.acquire(id2, LockMode.X);
        assertFalse(l2_3.isDone());

        lock.release(l1_1);
        l2_3.join();

        assertTrue(l2_3.id == id2 && l2_3.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests a queue upgrade.
     *
     * The lock sequence is X_lock(1), X_lock(2), S_lock(2), S_unlock(1).
     */
    @Test
    public void testQueuedUpgrade() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = new UUID(0, 0);
        UUID id2 = new UUID(0, 1);

        Locker l1_1 = lock.acquire(id1, LockMode.X);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.X);

        Locker l2_1 = lock.acquire(id2, LockMode.X);
        assertFalse(l2_1.isDone());

        Locker l2_2 = lock.acquire(id2, LockMode.S);
        assertFalse(l2_2.isDone());

        lock.release(l1_1);

        l2_1.join();
        l2_2.join();

        assertTrue(l2_1.id == id2 && l2_2.mode == LockMode.X);
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.X);

        lock.release(id2);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests a queue upgrade with supremum.
     *
     * The lock sequence is X_lock(1), S_lock(2), IX_lock(2), S_unlock(1).
     */
    @Test
    public void testBlockedUpgradeInverted2() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.X);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.X);

        Locker l2_1 = lock.acquire(id2, LockMode.S);
        assertFalse(l2_1.isDone());

        Locker l2_2 = lock.acquire(id2, LockMode.IX);
        assertFalse(l2_2.isDone());

        lock.release(l1_1);

        l2_1.join();
        l2_2.join();

        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.SIX);
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.SIX);

        lock.release(id2);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    @Test
    public void testBlockedUpgradeInverted3() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.IX);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.IX);

        Locker l2_1 = lock.acquire(id2, LockMode.S);
        assertFalse(l2_1.isDone());

        Locker l2_2 = lock.acquire(id2, LockMode.X);
        assertFalse(l2_2.isDone());

        lock.release(l1_1);

        l2_1.join();
        l2_2.join();

        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.X);
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.X);

        lock.release(id2);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests a queue upgrade.
     *
     * The lock sequence is X_lock(1), X_lock(2), X_lock(3), S_lock(2), S_unlock(1), X_unlock(2).
     */
    @Test
    public void testQueuedUpgradeWithIntermediate() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = new UUID(0, 0);
        UUID id2 = new UUID(0, 1);
        UUID id3 = new UUID(0, 2);

        Locker l1_1 = lock.acquire(id1, LockMode.X);
        l1_1.join();
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.X);

        Locker l2_1 = lock.acquire(id2, LockMode.X);
        assertFalse(l2_1.isDone());

        Locker l3_1 = lock.acquire(id3, LockMode.X);
        assertFalse(l3_1.isDone());

        Locker l2_2 = lock.acquire(id2, LockMode.S);
        assertFalse(l2_2.isDone());

        lock.release(l1_1);

        l2_1.join();
        lock.release(l2_1);

        l3_1.join();
        lock.release(l3_1);

        l2_2.join();

        assertTrue(l2_1.id == id2 && l2_1.mode == LockMode.X);
        assertTrue(l2_2.id == id2 && l2_2.mode == LockMode.S);
    }

    /**
     * Tests a downgrade to all lock modes.
     *
     * @param lockMode Lock mode.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testDowngrade(LockMode lockMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.X);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == LockMode.X);

        assertEquals(LockMode.X, lock.downgrade(id1, lockMode));

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(id1);
        assertTrue(lock.owners.isEmpty());
    }

    /**
     * Tests a upgrade downgrade scneraio.
     *
     * @param lockMode Lock mode.
     */
    @Test
    public void testUpgradeDowngrade() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1_1 = lock.acquire(id1, LockMode.S);
        assertNull(l1_1.join());
        assertTrue(l1_1.id == id1 && l1_1.mode == LockMode.S);

        Locker l1_2 = lock.acquire(id1, LockMode.IX);
        assertEquals(LockMode.S, l1_2.join());
        assertTrue(l1_2.id == id1 && l1_2.mode == LockMode.SIX);

        assertEquals(LockMode.SIX, lock.downgrade(id1, LockMode.S));

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(id1);
        assertTrue(lock.owners.isEmpty());
    }

    @Test
    public void testDowngradeToWeaker() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.SIX);
        l1.join();
        assertTrue(l1.id == id1 && l1.mode == LockMode.SIX);

        Locker l2 = lock.acquire(id2, LockMode.S);
        assertFalse(l2.isDone());

        assertEquals(LockMode.SIX, lock.downgrade(id1, LockMode.S));

        fail(); // l2.join(); // TODO FIXME !
    }

    @Test
    public void testInvalidRelease() {
        // TODO
    }
}
