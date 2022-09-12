package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HashIndexStoreTest {
    @Test
    public void testInsertScanRemove() {
        HashIndexStore<Integer> idx = new HashIndexStoreImpl();

        Tuple t = Tuple.create(1, "qqq");

        assertTrue(idx.insert(t, 1));

        assertEquals(1, idx.scan(t).next());

        assertTrue(idx.remove(t, 1));
        assertFalse(idx.remove(t, 1));
        assertFalse(idx.remove(t, 2));
    }

    @Test
    public void testInsertScanRemoveMany() {
        HashIndexStore<Integer> idx = new HashIndexStoreImpl();

        Tuple t1 = Tuple.create(1, "qqq");
        Tuple t2 = Tuple.create(1, "zzz");

        assertTrue(idx.insert(t1, 1));
        assertEquals(1, idx.scan(t1).next());

        assertTrue(idx.insert(t2, 1));
        assertEquals(1, idx.scan(t2).next());

        assertTrue(idx.remove(t1, 1));
        assertTrue(idx.remove(t2, 1));
    }
}
