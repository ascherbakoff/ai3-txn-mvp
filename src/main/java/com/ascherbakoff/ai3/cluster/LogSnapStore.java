package com.ascherbakoff.ai3.cluster;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.replication.Replicate;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LogSnapStore implements SnapStore {
    public LogSnapStore(boolean compaction) {
        // TODO compaction
        assert !compaction;
    }

    // The log.
    private final TreeMap<Timestamp, Replicate> log = new TreeMap<Timestamp, Replicate>();

    @Override
    public void put(Timestamp repTs, Replicate replicate) {
        log.put(repTs, replicate);
    }

    @Override
    public NavigableMap<Timestamp, Replicate> snapshot(Timestamp low, Timestamp high) {
        return new TreeMap(log.subMap(low, false, high, true));
    }

    @Override
    public Timestamp compactTs() {
        return Timestamp.min();
    }

    @Override
    public int logSize() {
        return log.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogSnapStore snapStore = (LogSnapStore) o;

        if (!log.equals(snapStore.log)) {
            return false;
        }

        return true;
    }
}
