package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Replicate;
import java.util.NavigableMap;

/**
 * An abstraction for storing replicated data in the snapshot + log form.
 * Snapshot represents a compacted log prefix up to compactTs.
 * Used for delta snapshot supporting.
 */
public interface SnapStore {
    void put(Timestamp repTs, Replicate replicate);

    NavigableMap<Timestamp, Replicate> snapshot(Timestamp low, Timestamp high);

    Timestamp compactTs();

    int logSize();
}
