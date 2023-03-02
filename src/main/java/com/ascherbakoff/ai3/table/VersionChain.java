package com.ascherbakoff.ai3.table;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import org.jetbrains.annotations.Nullable;

class VersionChain<T> {
    public static final int MAX_ALLOWED = 10;
    @Nullable Timestamp begin;
    @Nullable Timestamp end;
    T value;
    @Nullable
    UUID txId; // Lock holder for uncommitted version.
    @Nullable VersionChain<T> next;
    @Nullable VersionChain<T> prev;
    @Nullable VersionChain<T> last; // TODO makes sense only for chain head.
    private int cnt = 1; // TODO makes sense only for chain head.

    VersionChain(UUID txId, @Nullable Timestamp begin, @Nullable Timestamp end, @Nullable T value) {
        this.txId = txId;
        this.begin = begin;
        this.end = end;
        this.value = value;
    }

    private @Nullable Timestamp getBegin() {
        return begin;
    }

    private void setBegin(@Nullable Timestamp timestamp) {
        this.begin = timestamp;
    }

    private @Nullable Timestamp getEnd() {
        return end;
    }

    private void setEnd(@Nullable Timestamp end) {
        this.end = end;
    }

    private @Nullable T getValue() {
        return value;
    }

    private void setValue(@Nullable T value) {
        this.value = value;
    }

    private @Nullable VersionChain<T> getNext() {
        return next;
    }

    private void setNext(@Nullable VersionChain<T> next) {
        this.next = next;
    }

    private VersionChain<T> getPrev() {
        return prev;
    }

    private void setPrev(@Nullable VersionChain<T> prev) {
        this.prev = prev;
    }

    private VersionChain<T> getLast() {
        return last;
    }

    private void setLast(@Nullable VersionChain<T> last) {
        this.last = last;
    }

    private UUID getTxId() {
        return txId;
    }

    private void setTxId(@Nullable UUID txId) {
        this.txId = txId;
    }

    @Override
    public synchronized String toString() {
        return "VersionChain{" +
                "begin=" + begin +
                ", end=" + end +
                ", value=" + value +
                ", txId=" + txId +
                ", next=" + next +
                '}';
    }

    synchronized @Nullable T head() {
        return value;
    }

    synchronized @Nullable T resolve(@Nullable UUID txId, @Nullable Timestamp timestamp, @Nullable Predicate<T> filter) {
        assert txId == null ^ timestamp == null;

        if (timestamp == null) {
            assert this.txId == null || txId.equals(this.txId); // Must be enforced by locks.

            return filter == null ? value : filter.test(value) ? value : null;
        }

        VersionChain<T> cur = this;

        do {
            if (cur.begin != null && timestamp.compareTo(cur.begin) >= 0 && (cur.end == null || timestamp.compareTo(cur.end) < 0)) {
                return filter == null ? cur.value : filter.test(cur.value) ? cur.value : null;
            }

            cur = cur.next;
        } while(cur != null);

        return null;
    }

    /**
     * @param head The chain head.
     * @param val The value.
     * @param txId Txn id.
     */
    synchronized public T addWrite(T val, UUID txId) {
        if (txId.equals(this.txId)) {
            T oldVal = value;
            value = val;
            return oldVal;
        }

        T oldVal = this.value;

        // Re-link.
        VersionChain<T> next0 = new VersionChain<>(txId, begin, end, value);
        if (next != null)
            next.setPrev(next0);
        next0.setNext(next);
        next0.setPrev(this);
        setTxId(txId);
        setBegin(null);
        setEnd(null);
        setValue(val);
        setNext(next0);

        cnt++;

        if (cnt == 2) {
            setLast(next0);
        }

        // Cut tail.
        if (cnt > MAX_ALLOWED) {
            VersionChain<T> tail = getLast().getPrev();
            setLast(tail);
            tail.setNext(null);
            cnt--;
        }

        return oldVal;
    }

    synchronized public void printVersionChain() {
        System.out.println("head=" + (long)(hashCode() & (-1)));
        System.out.println("begin=" + begin + " end=" + end + ", value=" + value);

        VersionChain<T> next = this.next;

        while(next != null) {
            System.out.println("begin=" + next.begin + " end=" + next.end + ", value=" + next.value);

            next = next.next;
        }
    }

    synchronized public void commitWrite(Timestamp timestamp, UUID txId) {
        assert txId.equals(this.txId);

        Objects.requireNonNull(timestamp);

        setBegin(timestamp);
        setTxId(null);

        if (next != null)
            next.end = timestamp;
    }

    synchronized public void abortWrite(UUID txId) {
        assert txId.equals(this.txId);
        assert next != null;

        setTxId(null);

        this.begin = next.begin;
        this.end = next.end;
        this.value = next.value;
        this.next = next.next;
    }
}
