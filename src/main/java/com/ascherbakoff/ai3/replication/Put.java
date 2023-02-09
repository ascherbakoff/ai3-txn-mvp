package com.ascherbakoff.ai3.replication;

public class Put {
    private final Integer key;
    private final Integer value;

    public Put(Integer key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public Integer getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }
}
