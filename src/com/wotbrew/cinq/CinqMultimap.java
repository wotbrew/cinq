package com.wotbrew.cinq;

import clojure.lang.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;


public class CinqMultimap {

    // no nulls, multimap with robinhood linear probing. Suitable only for equi-joins where null!=null.

    float lf = 0.5f;
    int capacity;
    int entries;

    private int[] table;
    private Object[] keys;
    private List[] values;

    public CinqMultimap() {
        capacity = 16;
        table = new int[32];
        keys = new Object[16];
        values = new List[16];
        entries = 0;
    }

    static int hash(Object key) {
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    static boolean eq(Object key1, Object key2) {
        return key1.equals(key2);
        // much slower using equiv - consider fast option for common cases.
        //return CinqUtil.eq(key1, key2);
    }

    public int size() {
        return entries;
    }

    private void insertNode(int i, int h, Object key, Object value) {
        List<Object> al = new ArrayList<>();
        al.add(value);
        table[i * 2] = 1;
        table[(i * 2) + 1] = h;
        keys[i] = key;
        values[i] = al;
        entries++;
    }

    int psl(int h, int i, int m) {
        int expected = h & m;
        if (i < expected) {
            return (m - expected) + i;
        } else {
            return i - expected;
        }
    }

    private void doPutTuple(int h, Object key, Object value) {
        if (key == null) {
            return;
        }

        if (entries >= capacity * lf) {
            resize();
        }

        int m = (capacity - 1);
        int i = h & m;

        while (true) {
            if (table[i * 2] == 0) {
                insertNode(i, h, key, value);
                return;
            }

            int nh = table[(i * 2) + 1];

            if (psl(h, i, m) > psl(nh, i, m)) {
                Object nk = keys[i];
                List nv = values[i];
                insertNode(i, h, key, value);
                reinsertNode(i + 1, nh, nk, nv);
                return;
            }

            if (nh == h && eq(keys[i], key)) {
                values[i].add(value);
                return;
            }

            i = (i + 1) & m;
        }
    }

    private void reinsertNode(int i, int h, Object key, List value) {
        int m = (capacity - 1);
        i = i & m;
        for (; ; ) {
            if (table[i * 2] == 0) {
                table[i * 2] = 1;
                table[(i * 2) + 1] = h;
                keys[i] = key;
                values[i] = value;
                return;
            }

            int newHash = table[(i * 2) + 1];
            if (psl(h, i, m) > psl(newHash, i, m)) {

                Object newKey = keys[i];
                List newValue = values[i];

                table[(i * 2) + 1] = h;
                keys[i] = key;
                values[i] = value;

                h = newHash;
                key = newKey;
                value = newValue;
            }
            i = (i + 1) & m;
        }
    }

    private void resize() {
        int oldCapacity = capacity;
        int[] oldTable = table;
        Object[] oldKeys = keys;
        List[] oldValues = values;

        int newCapacity = oldCapacity * 2;
        table = new int[newCapacity * 2];
        keys = new Object[newCapacity];
        values = new List[newCapacity];
        capacity = newCapacity;

        for (int i = 0; i < oldCapacity; i++) {
            if (oldTable[i * 2] != 0) {
                int h = oldTable[(i * 2) + 1];
                reinsertNode(h & (newCapacity - 1), h, oldKeys[i], oldValues[i]);
            }
        }
    }

    public List get(Object key) {
        if (key == null) {
            return null;
        }
        int h = hash(key);
        int m = (capacity - 1);
        int i = h & m;

        for (; ; ) {

            if (table[i * 2] == 0) {
                return null;
            }

            int nh = table[(i * 2) + 1];

            if (nh == h && eq(keys[i], key)) {
                return values[i];
            }

            if (psl(h, i, m) > psl(nh, i, m)) {
                return null;
            }

            i = (i + 1) & m;
        }
    }

    public void put(Object key, Object tuple) {
        doPutTuple(hash(key), key, tuple);
    }

    public Object forEach(Function<Object, Object> f) {
        for (int i = 0; i < values.length; i++) {
            List value = values[i];
            if (value != null) {
                for (int j = 0; j < value.size(); j++) {
                    Object r = f.apply(value.get(j));
                    if (r != null) {
                        return f;
                    }
                }
            }
        }
        return null;
    }
}
