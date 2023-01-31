package com.ascherbakoff.ai3.cluster;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NodeId {
    private final String id;

    public NodeId(@NotNull String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NodeId nodeId = (NodeId) o;

        if (!id.equals(nodeId.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "NodeId{" +
                "id='" + id + '\'' +
                '}';
    }
}
