package com.ascherbakoff.ai3.lock;

import java.util.ArrayList;
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

        switch (test(locker)) {
            case 0: // Can lock immediately - requested lock is compatible with all held locks.
                locker.complete(null);
                owners.add(locker);

                break;
            case 1: // Reenter or upgrade
                locker.complete(null);

                break;
            default: // Wait
                waiters.add(locker);
        }

        return locker;
    }

    private int test(Locker locker) {
        for (Locker owner : owners) {
            boolean compat = owner.mode.compatible(locker.mode);

            // TODO FIXME map can be used for upgrade check
            if (owner.lockerId == locker.lockerId) {
                if (owner.mode.ordinal() >= locker.mode.ordinal() ||  // Allow reenter
                        owners.size() == 1 // Allow upgrade
                ) {
                    locker.mode = owner.mode;

                    return 1;
                } else {
                    return 2;
                }
            } else if (!compat)
                return 2;
        }

        return 0;
    }

    public synchronized void downgrade(LockMode mode) {

    }

    public synchronized void release(Locker locker) throws RuntimeException {
        assert owners.contains(locker);

        owners.remove(locker);

        if (owners.isEmpty() && !waiters.isEmpty()) {
            Locker w0 = waiters.remove(0);

            w0.complete(null);
            owners.add(w0);
        }
    }
}
