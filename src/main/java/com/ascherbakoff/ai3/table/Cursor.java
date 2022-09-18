package com.ascherbakoff.ai3.table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.Nullable;

public interface Cursor<T> {
    static Cursor EMPTY = new Cursor() {
        @Nullable
        @Override
        public Object next() {
            return null;
        }
    };

    @Nullable T next();

    default List<T> getAll() {
        List<T> values = new ArrayList<>();

        while(true) {
            T next = next();

            if (next == null)
                break;

            values.add(next);
        }

        return values;
    }

    static <T> Cursor<T> wrap(Iterator<T> iter) {
        return new Cursor<T>() {
            @Nullable
            @Override
            public T next() {
                if (!iter.hasNext())
                    return null;

                return iter.next();
            }
        };
    }
}
