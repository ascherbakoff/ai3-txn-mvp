package com.ascherbakoff.ai3.lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

public class Lock {
    /** Owners. */
    Map<UUID, Locker> owners = new HashMap<>();

    /** Waiters. */
    List<Locker> waiters = new ArrayList<>();

    /** Max version for deadlock prevention. */
    UUID maxVersion;

    private boolean fair;

    private DeadlockPrevention prevention;

    public Lock(boolean fair, DeadlockPrevention prevention) {
        this.fair = false;
        this.prevention = prevention;
    }

    /**
     * @param locker
     * @return True if a locker is compatible with all owners.
     */
    private boolean compatible(Locker locker) {
        for (Entry<UUID, Locker> e : owners.entrySet()) {
            if (e.getKey().equals(locker.id))
                continue;

            if (!e.getValue().mode.compatible(locker.mode))
                return false;
        }

        return true;
    }

    public synchronized Locker acquire(UUID lockerId, LockMode mode) throws LockException {
        Locker locker = new Locker(lockerId, mode);

        Locker owner = owners.get(lockerId);

        if (owner != null) {
            // Get a supremum.
            locker.mode = LockTable.supremum(owner.mode, locker.mode);

            // Check if a held lock is stronger or same as requested.
            if (owner.mode.ordinal() >= locker.mode.ordinal() ||  // Allow reenter
                    compatible(locker) // Allow immediate upgrade
            ) {
                owner.mode = locker.mode; // Overwrite locked mode.
                return owner;
            } else {
                if (prevention.forceOrder && maxVersion != null && maxVersion.compareTo(lockerId) < 0)
                    throw new LockException("Invalid lock order " + maxVersion + " -> " + lockerId);

                waiters.add(locker);
                return locker;
            }
        }

        if (!compatible(locker)) {
            if (prevention.forceOrder && maxVersion != null && maxVersion.compareTo(lockerId) < 0)
                throw new LockException("Invalid lock order " + maxVersion + " -> " + lockerId);

            waiters.add(locker);

            return locker;
        }

        // All owners are compatible.
        locker.complete(null);
        owners.put(lockerId, locker);

        if (maxVersion == null || lockerId.compareTo(maxVersion) < 0)
            maxVersion = lockerId;

        return locker;
    }

    public synchronized Locker downgrade(UUID lockerId, LockMode mode) {
        Locker locker = owners.get(lockerId);

        if (locker == null)
            throw new LockException("Bad locker");

        if (locker.mode.ordinal() < mode.ordinal())
            throw new LockException("Bad downgrade mode " + locker.mode + " -> " + mode);

        locker.mode = mode;

        return locker;
    }

    public synchronized void release(Locker locker) throws LockException {
        Locker removed = owners.remove(locker.id);

        if (removed == null)
            throw new LockException("Bad locker");

        // Handle delayed upgrade/reenter
        if (owners.size() == 1 && !waiters.isEmpty()) {
            Locker w = waiters.get(0);
            Locker o = owners.get(w.id);

            if (o != null) {
                // Get a supremum.
                w.mode = LockTable.supremum(o.mode, w.mode);

                owners.clear();
            }
        }

        if (owners.isEmpty() && !waiters.isEmpty()) {
            Locker w0 = waiters.remove(0);

            w0.complete(null);
            owners.put(w0.id, w0);
        }
    }
}
