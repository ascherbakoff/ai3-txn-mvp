package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.junit.jupiter.api.Test;

public class SortedIndexStoreTest {
    private static final Logger LOG = System.getLogger(SortedIndexStoreTest.class.getName());

    @Test
    public void testInsertScanRemove() {
        SortedIndexStore<Integer> idx = new SortedIndexStoreImpl<>();

        Tuple t1 = Tuple.create(1, "aaa");
        Tuple t2 = Tuple.create(1, "bbb");
        Tuple t3 = Tuple.create(2, "zzz");
        Tuple t4 = Tuple.create(3, "qqq");

        assertTrue(idx.insert(t1, 1));
        assertTrue(idx.insert(t2, 1));
        assertTrue(idx.insert(t3, 1));
        assertTrue(idx.insert(t4, 1));

        assertEquals(3, idx.scan(Tuple.create(1, "aaa"), true, Tuple.create(2, "zzz"), true).getAll().size());
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
