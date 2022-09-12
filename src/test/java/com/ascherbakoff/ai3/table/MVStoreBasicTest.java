package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

public abstract class MVStoreBasicTest {
    MVStoreImpl store;

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

        List<VersionChain<Tuple>> rows = store.query(new ScanQuery(), txId).loadAll(new ArrayList<>(3)).join();
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

    @Test
    public void testUniqueWithHistory() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        // TX1: id1 = insert [john, 100], TX1 // Insert tuple into row store
        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

//        // TX2: update id1, [john, 200], TX2 // Change salary for id1
        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.commit(txId2, Timestamp.now());

        // TX3: id2 = insert [bill, 100], TX3
        VersionChain<Tuple> rowId2 = store.insert(Tuple.create(0, "val2"), txId3).join(); // Insert must success.
        store.commit(txId3, Timestamp.now());
    }

    @Test
    public void testConcurrentTransactions() {
        UUID txId1 = new UUID(0, 0);
        store.insert(Tuple.create(0, "val0"), txId1).join();

        UUID txId2 = new UUID(0, 1);
        store.insert(Tuple.create(1, "val1"), txId2).join();

        List<VersionChain<Tuple>> rows1 = store.query(new ScanQuery(), txId1).loadAll(new ArrayList<>()).join();
        assertEquals(2, rows1.size());

        List<VersionChain<Tuple>> rows2 = store.query(new ScanQuery(), txId2).loadAll(new ArrayList<>()).join();
        assertEquals(2, rows2.size());
    }

    @Nullable
    Tuple getByIndex(UUID txId, int col, Tuple searchKey) {
        AsyncCursor<VersionChain<Tuple>> query = store.query(new EqQuery(col, searchKey), txId);
        List<VersionChain<Tuple>> rows = query.loadAll(new ArrayList<>()).join();
        return rows.isEmpty() ? null : store.get(rows.get(0), txId).join();
    }

    @Nullable
    Tuple getByIndex(Timestamp ts, int col, Tuple searchKey) {
        Cursor<Tuple> query = store.query(new EqQuery(col, searchKey), ts);
        List<Tuple> rows = query.getAll();
        return rows.isEmpty() ? null : rows.get(0);
    }
}