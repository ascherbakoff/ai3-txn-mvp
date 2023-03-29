package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;
import org.jetbrains.annotations.Nullable;

public class CollectResponse extends Response {
    private final Timestamp lwm;

    public CollectResponse(Timestamp lwm, Timestamp ts) {
        super(ts);
        this.lwm = lwm;
    }

    public CollectResponse(Timestamp ts, int ret) {
        super(ts, ret);
        this.lwm = null;
    }

    public @Nullable Timestamp getLwm() {
        return lwm; // null on error.
    }
}
