package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.ascherbakoff.ai3.clock.Timestamp;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

public class MVStoreTest {
    private MVStoreImpl store = new MVStoreImpl(new int[][]{{0}});

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
    }

    @Test
    public void testAbort() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        store.insert(Tuple.create(1, "val1"), txId).join();
        store.insert(Tuple.create(2, "val2"), txId).join();

        store.abort(txId);
        assertNull(store.txnLocalMap.get(txId));
    }

    @Test
    public void testInsertDuplicate() {
        UUID txId = new UUID(0, 0);

        store.insert(Tuple.create(0, "val0"), txId).join();
        var err = assertThrows(CompletionException.class, () -> store.insert(Tuple.create(0, "val0"), txId).join());
        assertEquals(UniqueException.class, err.getCause().getClass());
    }
}
