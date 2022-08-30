package com.ascherbakoff.ai3.lock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 *
 */
public class LockTableTest {
    private LockTable lockTable = new LockTable(10);

    /**
     * Tests lock/unlock for all lock modes.
     */
    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testLockUnlock(LockMode lockMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, lockMode);
        l1.join();

        assertTrue(l1.lockerId == id1 && l1.mode == lockMode);

        lock.release(l1);

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

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
        assertTrue(l1.lockerId == id1 && l1.mode == lockMode);

        Locker l2 = lock.acquire(id2, lockMode);

        assertFalse(l2.isDone());
        assertTrue(lock.owners.size() == 1);
        assertFalse(lock.waiters.isEmpty());

        lock.release(l1);
        l2.join();
        assertTrue(l2.lockerId == id2 && l2.mode == lockMode);

        assertTrue(l2.isDone());
        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l2);
        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests if compatible locks can be acquired in the same time.
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

        assertTrue(l1.lockerId == id1 && l1.mode == lockMode);

        Locker l2 = lock.acquire(id2, lockMode);
        l2.join();

        assertTrue(lock.owners.size() == 2);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l1);

        assertTrue(lock.owners.size() == 1);

        lock.release(l2);
        assertTrue(lock.owners.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(LockMode.class)
    public void testReenter(LockMode lockMode) {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.X);
        l1.join();
        assertTrue(l1.lockerId == id1 && l1.mode == LockMode.X);

        Locker l2 = lock.acquire(id1, lockMode);
        l2.join();
        assertTrue(l2.lockerId == id1 && l2.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        lock.release(l1);
        assertTrue(lock.owners.isEmpty());
    }

    @Test
    public void testInvalidRelease() {
        // TODO
    }

    /**
     * Tests direct upgrade S_lock, X_lock for the same locker.
     *
     * @throws Exception
     */
    @Test
    @Disabled
    public void testDirectUpgrade() throws Exception {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.S);
        l1.join();
        assertTrue(l1.lockerId == id1 && l1.mode == LockMode.S);

        Locker l2 = lock.acquire(id1, LockMode.X);
        l2.get(5, TimeUnit.SECONDS);

        assertTrue(l2.lockerId == id1 && l2.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        assertThrows(Exception.class, () -> lock.release(l1), "Illegal lock type for release");

        lock.release(l2); // We hold X lock

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests direct upgrade IS_lock, IX_lock, X_lock for the same locker.
     *
     * @throws Exception
     */
    @Test
    @Disabled
    public void testDirectUpgradeMulti() throws Exception {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.IS);
        l1.join();
        assertTrue(l1.lockerId == id1 && l1.mode == LockMode.IS);

        Locker l2 = lock.acquire(id1, LockMode.IX);
        l2.join();
        assertTrue(l2.lockerId == id1 && l2.mode == LockMode.IX);

        Locker l3 = lock.acquire(id1, LockMode.X);
        l3.get(5, TimeUnit.SECONDS);
        assertTrue(l3.lockerId == id1 && l3.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());

        assertThrows(Exception.class, () -> lock.release(l1), "Illegal lock type for release");
        assertThrows(Exception.class, () -> lock.release(l2), "Illegal lock type for release");

        lock.release(l3); // We hold X lock

        assertTrue(lock.owners.isEmpty());
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests delayed upgrade S_lock(1), S_lock(2), S_unlock(1), X_lock(2) for two lockers.
     */
    @Test
    @Disabled
    public void testDelayedUpgrade() {
        Lock lock = lockTable.getOrAddEntry(0);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        Locker l1 = lock.acquire(id1, LockMode.S);
        l1.join();
        assertTrue(l1.lockerId == id1 && l1.mode == LockMode.S);

        Locker l2 = lock.acquire(id2, LockMode.S);
        l2.join();
        assertTrue(l2.lockerId == id2 && l1.mode == LockMode.S);

        Locker l3 = lock.acquire(id2, LockMode.X);
        assertFalse(l3.isDone());

        lock.release(l1);
        l3.join();

        assertTrue(l3.lockerId == id2 && l3.mode == LockMode.X);

        assertTrue(lock.owners.size() == 1);
        assertTrue(lock.waiters.isEmpty());
    }

    /**
     * Tests delayed upgrade IS_lock(1), IS_lock(2), IX_lock(2), IS_unlock(1), X_lock(2) for two lockers.
     */
    @Test
    public void testDelayedUpgradeMulti() {
        // TODO
    }

    @Test
    public void testDowngrade() {

    }
}
