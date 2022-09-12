package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.ascherbakoff.ai3.clock.Timestamp;
import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

public class MVStoreWithHashUniqueIndexTest extends MVStoreBasicTest {
    public MVStoreWithHashUniqueIndexTest() {
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
    public void testUniqueWithHistory2() {
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
    public void testUniqueWithHistory3() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);
        UUID txId4 = new UUID(0, 4);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, Timestamp.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.commit(txId2, Timestamp.now());

        store.insert(Tuple.create(0, "val3"), txId4).join();

//        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(0, "val3"), txId4).join());
//        assertEquals(UniqueException.class, err.getCause().getClass());

        store.update(rowId, Tuple.create(0, "val2"), txId3).join();
        store.commit(txId3, Timestamp.now());

        store.commit(txId4, Timestamp.now());

        // TODO FIXME unique violation.
        fail();
    }
}
