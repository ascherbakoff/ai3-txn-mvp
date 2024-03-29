package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.ascherbakoff.ai3.lock.DeadlockPrevention;
import com.ascherbakoff.ai3.lock.LockTable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public abstract class MVStoreBasicNonUniqueIndexTest extends MVStoreBasicTest {
    public MVStoreBasicNonUniqueIndexTest() {
        {
            VersionChainRowStore<Tuple> rowStore = new VersionChainRowStore<>();
            store = new MVStoreImpl(
                    rowStore,
                    new LockTable(10, true, DeadlockPrevention.none()),
                    Map.of(0, new HashNonUniqueIndex(0, new LockTable(10, true, DeadlockPrevention.none()), new HashIndexStoreImpl<>(), rowStore))
            );
        }
    }

    @Test
    public void testInsertDuplicate() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(0, "val0"), txId).join();
    }

    @Test
    public void testInsertDuplicate_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(0, "val0"), txId2).join();

        store.commit(txId, clock.now());
        store.commit(txId2, clock.now());
    }

    @Test
    public void testInsertGet_2TX() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);

        store.insert(Tuple.create(0, "val0"), txId).join();
        assertEquals(Tuple.create(0, "val0"), getSingle(txId, 0, Tuple.create(0)));

        CompletableFuture<VersionChain<Tuple>> fut = store.insert(Tuple.create(0, "val0"), txId2);
        assertFalse(fut.isDone());

        store.commit(txId, clock.now());

        fut.join();

        store.commit(txId2, clock.now());
    }

    @Test
    public void testUniqueWithHistory2() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);
        UUID txId4 = new UUID(0, 4);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        store.commit(txId, clock.now());

        store.update(rowId, Tuple.create(1, "val1"), txId2).join();
        store.commit(txId2, clock.now());

        store.update(rowId, Tuple.create(0, "val2"), txId3).join();
        store.commit(txId3, clock.now());

        // TX3: id2 = insert [bill, 100], TX3
        store.insert(Tuple.create(0, "val3"), txId4).join();
        store.commit(txId4, clock.now());
    }

    @Test
    public void testConcurrentInsertAbort() {
        UUID txId = new UUID(0, 0);
        UUID txId2 = new UUID(0, 1);
        UUID txId3 = new UUID(0, 2);

        VersionChain<Tuple> rowId = store.insert(Tuple.create(0, "val0"), txId).join();
        VersionChain<Tuple> rowId2 = store.insert(Tuple.create(0, "val0"), txId2).join();

        store.abort(txId);
        store.abort(txId2);

        assertNull(store.txnLocalMap.get(txId));
        assertNull(store.txnLocalMap.get(txId2));

        assertNull(getSingle(txId3, 0, Tuple.create(0)));
    }
}
