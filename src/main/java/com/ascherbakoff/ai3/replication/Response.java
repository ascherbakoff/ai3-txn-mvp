package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Response {
    private Timestamp ts;
    private int ret;
    private String message;

    public Response(Timestamp ts) {
        this(ts, 0, null);
    }

    public Response(Timestamp ts, int ret, String message) {
        this.ts = ts;
        this.ret = ret;
        this.message = message;
    }

    public int getReturn() {
        return ret;
    }

    public String getMessage() {
        return message;
    }

    public Timestamp getTs() {
        return ts;
    }
}
