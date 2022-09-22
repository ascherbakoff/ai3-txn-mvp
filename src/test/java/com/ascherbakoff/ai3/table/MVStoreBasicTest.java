package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

public abstract class MVStoreBasicTest {
    MVStoreImpl store;

    @Test
    public void testInsert() {
        UUID txId = new UUID(0, 0);

        Tuple t0 = Tuple.create(0, "val0");
        store.insert(t0, txId).join();
        Tuple t1 = Tuple.create(1, "val1");
        store.insert(t1, txId).join();
        Tuple t2 = Tuple.create(2, "val2");
        store.insert(t2, txId).join();

        assertEquals(3, store.txnLocalMap.get(txId).locks.size());
        assertEquals(3, store.txnLocalMap.get(txId).writes.size());

        store.commit(txId, Timestamp.now());
        assertNull(store.txnLocalMap.get(txId));

        List<VersionChain<Tuple>> rows = store.query(new ScanQuery(), txId).loadAll(new ArrayList<>(3)).join();
        assertEquals(3, rows.size());

        assertEquals(t0, getSingle(txId, 0, Tuple.create(0)));
        assertEquals(t1, getSingle(txId, 0, Tuple.create(1)));
        assertEquals(t2, getSingle(txId, 0, Tuple.create(2)));
    }

    @Test
    public void testInsertRemoveMultiTxn() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);
        UUID txId4 = new UUID(0, 3);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());
        store.update(rowId, Tuple.create(1, "val1"), txId2).join();

        assertEquals(Tuple.create(1, "val1"), getSingle(txId2, 0, Tuple.create(1)));

        store.commit(txId2, Timestamp.now());
        store.update(rowId, Tuple.TOMBSTONE, txId3).join();
        store.commit(txId3, Timestamp.now());

        assertNull(getSingle(txId4, 0, Tuple.create(0)));
        assertNull(getSingle(txId4, 0, Tuple.create(1)));
    }

    @Test
    public void testAbort() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        store.abort(txId);
        assertNull(store.txnLocalMap.get(txId));

        assertNull(getSingle(txId, 0, Tuple.create(0)));
        assertNull(getSingle(txId, 0, Tuple.create(1)));
        assertNull(getSingle(txId, 0, Tuple.create(2)));
    }

    @Test
    public void testRemove() {
        UUID txId = new UUID(0, 0);

        VersionChain<Tuple> rowId1 = store.insert(Tuple.create(0, "val0"), txId).join();
        VersionChain<Tuple> rowId2 = store.insert(Tuple.create(1, "val1"), txId).join();
        VersionChain<Tuple> rowId3 = store.insert(Tuple.create(2, "val2"), txId).join();

        store.update(rowId1, Tuple.TOMBSTONE, txId).join();
        store.update(rowId2, Tuple.TOMBSTONE, txId).join();
        store.update(rowId3, Tuple.TOMBSTONE, txId).join();

        assertNull(getSingle(txId, 0, Tuple.create(0)));
        assertNull(getSingle(txId, 0, Tuple.create(1)));
        assertNull(getSingle(txId, 0, Tuple.create(2)));
    }

    @Test
    public void testInsertAbortCleanupNoHistory() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.abort(txId2);

        assertEquals(Tuple.create(0, "val0"), getSingle(txId3, 0, Tuple.create(0)));
        assertNull(getSingle(txId3, 0, Tuple.create(1)));
    }

    @Test
    public void testInsertRemoveCleanupNoHistory() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.update(rowId, Tuple.TOMBSTONE, txId2).join();
        store.commit(txId2, Timestamp.now());

        assertNull(getSingle(txId3, 0, Tuple.create(0)));
        assertNull(getSingle(txId3, 0, Tuple.create(1)));
    }

    @Test
    public void testInsertAbortRetainHistory() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.update(rowId, Tuple.create(1, "val1"), txId).join();
        Timestamp t1 = Timestamp.now();
        store.commit(txId, t1);

        store.insert(Tuple.create(0, "val2"), txId2).join();
        store.abort(txId2); // Abort should not remove historical value.

        assertEquals(1, store.query(new EqQuery(0, Tuple.create(0)), txId3).loadAll(new ArrayList<>()).join().size());

        assertNull(getSingle(txId3, 0, Tuple.create(0)));
        assertEquals(Tuple.create(1, "val1"), getSingle(txId3, 0, Tuple.create(1)));
    }

    @Test
    public void testIndexQuery() {
        UUID txId1 = new UUID(0, 0);

        Tuple t1 = Tuple.create(0, "val0");
        store.insert(t1, txId1).join();
        Tuple t2 = Tuple.create(1, "val1");
        store.insert(t2, txId1).join();

        assertEquals(t1, getSingle(txId1, 0, Tuple.create(0)));
        assertEquals(t2, getSingle(txId1, 0, Tuple.create(1)));

        store.commit(txId1, Timestamp.now());

        assertEquals(t1, getSingle(txId1, 0, Tuple.create(0)));
        assertEquals(t2, getSingle(txId1, 0, Tuple.create(1)));
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
        store.insert(Tuple.create(0, "val2"), txId3).join(); // Insert must success.
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

    @Test
    public void testInsertGetUpper_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txI2 = new UUID(0, 2);
        UUID txI3 = new UUID(0, 3);

        store.insert(Tuple.create(10, "val1"), txId).join();
        store.commit(txId, Timestamp.now());

        store.insert(Tuple.create(9, "val2"), txI2).join();

        assertEquals(Tuple.create(10, "val1"), getSingle(txI3, 0, Tuple.create(10)));
        CompletableFuture<Tuple> fut = getSingleAsync(txI3, 0, Tuple.create(9));
        assertFalse(fut.isDone());
    }

    /**
     * Returns exactly one value by EqQuery (or null if not found)
     *
     * @param txId Tx id.
     * @param col Column.
     * @param searchKey Search key.
     * @return The tuple.
     */
    @Nullable
    Tuple getSingle(UUID txId, int col, Tuple searchKey) {
        return getSingleAsync(txId, col, searchKey).join();
    }

    CompletableFuture<Tuple> getSingleAsync(UUID txId, int col, Tuple searchKey) {
        AsyncCursor<VersionChain<Tuple>> query = store.query(new EqQuery(col, searchKey), txId);
        return query.loadAll(new ArrayList<>()).thenCompose(new Function<List<VersionChain<Tuple>>, CompletionStage<Tuple>>() {
            @Override
            public CompletableFuture<Tuple> apply(List<VersionChain<Tuple>> heads) {
                CompletableFuture<List<Tuple>> f = CompletableFuture.completedFuture(new ArrayList<>());

                for (VersionChain<Tuple> rowId : heads) {
                    f = f.thenCompose(res -> {
                        return store.get(rowId, txId, tup -> tup == Tuple.TOMBSTONE ? false : tup.select(0).equals(searchKey))
                                .thenApply(t -> {
                                    if (t != null) {
                                        res.add(t);
                                    }

                                    return res;
                                });
                    });
                }

                return f.thenApply(rows -> {
                    assertTrue(rows.size() <= 1);
                    return rows.isEmpty() ? null : rows.get(0);
                });
            }
        });
    };

    @Nullable
    Tuple getSingle(Timestamp ts, int col, Tuple searchKey) {
        Cursor<Tuple> query = store.query(new EqQuery(col, searchKey), ts);
        List<Tuple> rows = query.getAll();
        assertTrue(rows.size() <= 1);
        return rows.isEmpty() ? null : rows.get(0);
    }
}
