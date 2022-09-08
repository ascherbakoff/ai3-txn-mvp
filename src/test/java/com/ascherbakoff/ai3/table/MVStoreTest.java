package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

public class MVStoreTest {
    private MVStoreImpl store = new MVStoreImpl(
            new VersionChainRowStore<>(new LockTable(10, true, DeadlockPrevention.none())),
            Map.of(0, new HashIndexImpl<>(new LockTable(10, true, DeadlockPrevention.none()), true)), // PK on first column.
            Map.of()
            );

    @Test
    public void testInsert() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        assertEquals(3, store.txnLocalMap.get(txId).locks.size());
        assertEquals(3, store.txnLocalMap.get(txId).writes.size());

        store.commit(txId, Timestamp.now());
        assertNull(store.txnLocalMap.get(txId));

        List<Tuple> rows = store.query(new ScanQuery(), txId).loadAll(new ArrayList<>(3)).join();
        assertEquals(3, rows.size());

        assertEquals(Tuple.create(0, "val0"), getByIndex(txId, 0, Tuple.create(0)));
        assertEquals(Tuple.create(1, "val1"), getByIndex(txId, 0, Tuple.create(1)));
        assertEquals(Tuple.create(2, "val2"), getByIndex(txId, 0, Tuple.create(2)));
    }

    @Test
    public void testAbort() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        store.abort(txId);
        assertNull(store.txnLocalMap.get(txId));

        assertNull(getByIndex(txId, 0, Tuple.create(0)));
        assertNull(getByIndex(txId, 0, Tuple.create(1)));
        assertNull(getByIndex(txId, 0, Tuple.create(2)));
    }

    @Test
    public void testInsertDuplicate() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(0, "val0"), txId).join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }

    @Test
    public void testConcurrentTransactions() {
        UUID txId1 = new UUID(0, 0);
        store.insert(Tuple.create(0, "val0"), txId1).join();

        UUID txId2 = new UUID(0, 1);
        store.insert(Tuple.create(1, "val1"), txId2).join();

        assertEquals(1, store.query(new ScanQuery(), txId1).loadAll(new ArrayList<>()).join().size());
        assertEquals(1, store.query(new ScanQuery(), txId2).loadAll(new ArrayList<>()).join().size());
    }

    @Test
    public void testIndexQuery() {
        UUID txId1 = new UUID(0, 0);

        Tuple t1 = Tuple.create(0, "val0");
        store.insert(t1, txId1).join();
        Tuple t2 = Tuple.create(1, "val1");
        store.insert(t2, txId1).join();

        assertEquals(t1, getByIndex(txId1, 0, Tuple.create(0)));
        assertEquals(t2, getByIndex(txId1, 0, Tuple.create(1)));

        store.commit(txId1, Timestamp.now());

        assertEquals(t1, getByIndex(txId1, 0, Tuple.create(0)));
        assertEquals(t2, getByIndex(txId1, 0, Tuple.create(1)));
    }

    @Nullable
    private Tuple getByIndex(UUID txId, int col, Tuple searchKey) {
        List<Tuple> rows = store.query(new EqQuery(col, searchKey), txId).loadAll(new ArrayList<>()).join();
        return rows.isEmpty() ? null : rows.get(0);
    }
}
