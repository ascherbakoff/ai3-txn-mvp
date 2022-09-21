package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

public abstract class MVStoreWithUniqueIndexBasicTest extends MVStoreBasicTest {
    public MVStoreWithUniqueIndexBasicTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new HashUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new HashIndexStoreImpl<>(), rowStore))
            );
        }
    }

    @Test
    public void testInsertDuplicate() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(0, "val0"), txId).join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }

    @Test
    public void testInsertDuplicate_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        store.insert(Tuple.create(0, "val0"), txId).join();

        CompletableFuture<?> fut = store.insert(Tuple.create(0, "val0"), txId2);
        assertFalse(fut.isDone());

        store.commit(txId, Timestamp.now());

        var err = assertThrows(CompletionException.class, () -> {
            fut.join();
        });
        assertEquals(UniqueException.class, err.getCause().getClass());
    }

    @Test
    public void testUniqueWithHistoryABA() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);
        UUID txId4 = new UUID(0, 4);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.commit(txId2, Timestamp.now());

        store.update(rowId, Tuple.create(0, "val2"), txId3).join();
        store.commit(txId3, Timestamp.now());

        // TX3: id2 = insert [bill, 100], TX3
        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(0, "val3"), txId4).join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }

    @Test
    public void testInsertUniqueWithUncommitted() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.update(rowId, Tuple.create(0, "val2"), txId2).join();

        assertEquals(Tuple.create(0, "val2"), getSingle(txId2, 0, Tuple.create(0)));

        store.commit(txId2, Timestamp.now());

        assertEquals(Tuple.create(0, "val2"), getSingle(txId2, 0, Tuple.create(0)));
    }

    @Test
    public void testUniqueWithHistoryABA_Reorder() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);
        UUID txId4 = new UUID(0, 4);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.commit(txId2, Timestamp.now());

        Tuple res = Tuple.create(0, "val3");
        store.insert(res, txId4).join();

        CompletableFuture<Tuple> fut = store.update(rowId, Tuple.create(0, "val2"), txId3);
        assertFalse(fut.isDone());

        store.commit(txId4, Timestamp.now());

        var err = assertThrows(CompletionException.class, () -> fut.join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }

    @Test
    public void testInsertGet_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        store.insert(Tuple.create(0, "val0"), txId).join();

        CompletableFuture<Tuple> fut = getSingleAsync(txId2, 0, Tuple.create(0));

        store.commit(txId, Timestamp.now());

        assertEquals(Tuple.create(0, "val0"), fut.join());
    }

    @Test
    public void testGetInsert_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        assertNull(getSingle(txId2, 0, Tuple.create(0)));

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(0, "val0"), txId);
        assertFalse(fut.isDone());

        assertNull(getSingle(txId2, 0, Tuple.create(0)));

        store.commit(txId2, Timestamp.now());

        fut.join();

        assertEquals(Tuple.create(0, "val0"), getSingle(txId, 0, Tuple.create(0)));
    }

    @Test
    public void testInsertGetAbort_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        store.insert(Tuple.create(0, "val0"), txId).join();

        CompletableFuture<Tuple> fut = getSingleAsync(txId2, 0, Tuple.create(0));

        store.abort(txId);

        assertNull(fut.join());
    }

    @Test
    public void testInsertUpdateRemove_2TX() throws InterruptedException {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(0, "val1"), txId2).join();

        CompletableFuture<Tuple> fut = store.remove(rowId, txId3);
        assertFalse(fut.isDone());
        CompletableFuture<Tuple> fut2 = getSingleAsync(txId3, 0, Tuple.create(0));
        assertFalse(fut2.isDone());

        store.commit(txId2, Timestamp.now());

        assertEquals(Tuple.create(0, "val1"), fut.join());
        fut2.join(); // Both futures are concurrent
        assertNull(getSingle(txId3, 0, Tuple.create(0)));
    }

    @Test
    public void testInsertUpdateRemoveAbort_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(0, "val1"), txId2).join();

        CompletableFuture<Tuple> fut = store.remove(rowId, txId3);
        assertFalse(fut.isDone());
        CompletableFuture<Tuple> fut2 = getSingleAsync(txId3, 0, Tuple.create(0));
        assertFalse(fut2.isDone());

        store.abort(txId2);

        assertEquals(Tuple.create(0, "val0"), fut.join());
        fut2.join(); // Both futures are concurrent
        assertNull(getSingle(txId3, 0, Tuple.create(0)));
    }

    @Test
    public void testRemoveInsert_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.remove(rowId, txId2).join();

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(0, "val1"), txId3);
        assertFalse(fut.isDone());
        CompletableFuture<Tuple> fut2 = getSingleAsync(txId3, 0, Tuple.create(0));
        assertFalse(fut2.isDone());

        store.commit(txId2, Timestamp.now());

        fut.join();
        fut2.join();
        assertEquals(Tuple.create(0, "val1"), getSingle(txId3, 0, Tuple.create(0)));
    }

    @Test
    public void testRemoveInsertAbort_2TX() throws InterruptedException {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.remove(rowId, txId2).join();

        CompletableFuture<Tuple> fut2 = getSingleAsync(txId3, 0, Tuple.create(0));
        assertFalse(fut2.isDone());

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(0, "val1"), txId3);
        assertFalse(fut.isDone());

        store.abort(txId2);

        assertEquals(Tuple.create(0, "val0"), fut2.join());
        var err = assertThrows(CompletionException.class, () -> fut.join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }
}
