package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.jetbrains.annotations.Nullable;

public class SnapshotResponse extends Response {
    private final NavigableMap<Timestamp, Replicate> snapshot;
    private final Timestamp current;

    /**
     * Null "current" means a stable replication.
     */
    public SnapshotResponse(Timestamp hlc, NavigableMap<Timestamp, Replicate> snapshot, Timestamp current) {
        super(hlc);
        this.snapshot = snapshot;
        this.current = current;
    }

    public NavigableMap<Timestamp, Replicate> getSnapshot() {
        return snapshot;
    }

    public @Nullable Timestamp getCurrent() {
        return current;
    }
}
