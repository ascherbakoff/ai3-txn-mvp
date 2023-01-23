package com.ascherbakoff.ai3.replication;

import com.ascherbakoff.ai3.clock.Timestamp;

public class Request {
    private Timestamp ts;

    Type type;
    private Timestamp lwm;
    private Object payload;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public void setTs(Timestamp ts) {
        this.ts = ts;
    }

    public Timestamp getTs() {
        return ts;
    }

    public Timestamp getLwm() {
        return lwm;
    }

    public void setLwm(Timestamp lwm) {
        this.lwm = lwm;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Object getPayload() {
        return payload;
    }

    public enum Type {
        SYNC, DATA
    }
}
