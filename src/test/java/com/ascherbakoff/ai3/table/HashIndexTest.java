package com.ascherbakoff.ai3.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class HashIndexTest {
    @Test
    public void testInsertScanRemove() {
        HashIndex<Integer> idx = new HashIndexImpl(true);

        Tuple t = Tuple.create(1, "qqq");

        assertTrue(idx.insert(t, 1));

        assertEquals(1, idx.scan(t).next());

        assertTrue(idx.remove(t, 1));
        assertFalse(idx.remove(t, 1));
        assertFalse(idx.remove(t, 2));
    }

    @Test
    public void testInsertScanRemoveMany() {
        HashIndex<Integer> idx = new HashIndexImpl(true);

        Tuple t1 = Tuple.create(1, "qqq");
        Tuple t2 = Tuple.create(1, "zzz");

        assertTrue(idx.insert(t1, 1));
        assertEquals(1, idx.scan(t1).next());

        assertTrue(idx.insert(t2, 1));
        assertEquals(1, idx.scan(t2).next());

        assertTrue(idx.remove(t1, 1));
        assertTrue(idx.remove(t2, 1));
    }

    @Test
    public void testInsertUnique() {
        HashIndex<Integer> idx = new HashIndexImpl(true);

        Tuple t = Tuple.create(1, "qqq");

        assertTrue(idx.insert(t, 1));

        assertEquals(1, idx.scan(t).next());

        assertFalse(idx.insert(t, 2));
    }

    @Test
    public void testInsertNonUnique() {
        HashIndex<Integer> idx = new HashIndexImpl(false);

        Tuple t = Tuple.create(1, "qqq");

        assertTrue(idx.insert(t, 1));

        Iterator<Integer> iter = idx.scan(t);
        assertTrue(iter.hasNext());
        assertEquals(1, iter.next());

        assertTrue(idx.insert(t, 2));

        Set<Integer> values = idx.getAll(t);
        assertEquals(2, values.size());
    }
}
