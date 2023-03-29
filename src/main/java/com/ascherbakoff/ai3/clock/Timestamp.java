package com.ascherbakoff.ai3.clock;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

public class Timestamp implements Comparable<Timestamp> {
    private final long physical;

    private final long counter;

    Timestamp(long physical, long counter) {
        this.physical = physical;
        this.counter = counter;
    }

    public long physical() {
        return physical;
    }

    public long counter() {
        return counter;
    }

    public Timestamp adjust(long delta) {
        assert delta > 0;
        return new Timestamp(physical + delta, 0);
    }

    public static Timestamp min() {
        return new Timestamp(0, 0);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Timestamp timestamp = (Timestamp) o;

        if (physical != timestamp.physical) {
            return false;
        }
        if (counter != timestamp.counter) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (physical ^ (physical >>> 32));
        result = 31 * result + (int) (counter ^ (counter >>> 32));
        return result;
    }

    @Override
    public int compareTo(Timestamp other) {
        if (this.physical == other.physical) {
            return Long.compare(this.counter, other.counter);
        }

        return Long.compare(this.physical, other.physical);
    }

    @Override
    public String toString() {
        return "[" + physical + ":" + counter + "]";
    }

    public UUID toUUID() {
        return new UUID(physical(), counter());
    }
}
