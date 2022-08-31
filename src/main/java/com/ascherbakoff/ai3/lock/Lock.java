package com.ascherbakoff.ai3.lock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class Lock {
    /** Owners. */
    List<Locker> owners = new ArrayList<>();

    /** Waiters. */
    List<Locker> waiters = new ArrayList<>();

    List<Locker> owners() {
        assert !owners.isEmpty();

        return owners;
    }

    public synchronized Locker acquire(UUID lockerId, LockMode mode) {
        Locker locker = new Locker(lockerId, mode);

        // TODO FIXME linear complexity, use Map ?
        for (Locker owner : owners) {
            if (owner.id == locker.id) {
                // Calculate a supremum.
                locker.mode = LockTable.UPGRADE_MATRIX[owner.mode.ordinal()][locker.mode.ordinal()];

                // Check if a held lock is stronger or same as requested.
                if (owner.mode.ordinal() >= locker.mode.ordinal() ||  // Allow reenter
                        owners.size() == 1 // Allow immediate upgrade
                ) {
                    owner.mode = locker.mode; // Overwrite locked mode.
                    return owner;
                } else {
                    waiters.add(locker);
                    return locker;
                }
            }
        }

        for (Locker owner : owners) {
            assert owner.id != locker.id;

            if (!owner.mode.compatible(locker.mode)) {
                waiters.add(locker);

                return locker;
            }
        }

        // All owners are compatible.
        locker.complete(null);
        owners.add(locker);

        return locker;
    }

    public synchronized void downgrade(Locker locker, LockMode mode) {

    }

    public synchronized void release(Locker locker) throws RuntimeException {
        Iterator<Locker> it = owners.iterator();

        // TODO FIXME linear - bad
        while (it.hasNext()) {
            Locker owner = it.next();

            if (owner.id == locker.id)
                it.remove();
        }

        // Handle delayed upgrade/reenter
        if (owners.size() == 1 && !waiters.isEmpty()) {
            Locker o = owners.get(0);
            Locker w = waiters.get(0);

            if (o.id == w.id) {
                // Calculate a supremum.
                w.mode = LockTable.UPGRADE_MATRIX[o.mode.ordinal()][w.mode.ordinal()];

                owners.clear();
            }
        }

        if (owners.isEmpty() && !waiters.isEmpty()) {
            Locker w0 = waiters.remove(0);

            w0.complete(null);
            owners.add(w0);
        }
    }
}
