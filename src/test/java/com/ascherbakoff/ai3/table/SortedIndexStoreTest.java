package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class SortedIndexStoreTest {
    private static final Logger LOG = System.getLogger(SortedIndexStoreTest.class.getName());

    @Test
    public void testInsertScanRemove() {
        SortedIndexStoreImpl<Integer> idx = new SortedIndexStoreImpl<>();

        Tuple t1 = Tuple.create(1, "aaa");
        Tuple t2 = Tuple.create(1, "bbb");
        Tuple t3 = Tuple.create(2, "zzz");
        Tuple t4 = Tuple.create(3, "qqq");

        assertTrue(idx.insert(t1, 1));
        assertTrue(idx.insert(t1, 2));
        assertTrue(idx.insert(t2, 3));
        assertTrue(idx.insert(t3, 4));
        assertTrue(idx.insert(t4, 5));

        List<Map.Entry<Tuple, Cursor<Integer>>> rows = idx.scan(Tuple.create(1, "aaa"), true, Tuple.create(1, "bbb"), true).getAll();
        assertEquals(2, rows.size());
        assertEquals(t1, rows.get(0).getKey());
        assertEquals(2, rows.get(0).getValue().getAll().size());
        assertEquals(t2, rows.get(1).getKey());
        assertEquals(1, rows.get(1).getValue().getAll().size());

        assertFalse(idx.remove(t1, 3));
        assertTrue(idx.remove(t1, 1));
        assertTrue(idx.remove(t1, 2));
        assertTrue(idx.remove(t2, 3));
        assertTrue(idx.remove(t3, 4));
        assertTrue(idx.remove(t4, 5));

        assertTrue(idx.data.isEmpty());

        //assertTrue(idx.data.isEmpty());
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
