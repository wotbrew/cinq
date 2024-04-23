package com.wotbrew.cinq;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class CinqMultimap {
    // linear probing (robin-hood) multi-map for now

    int entries;
    List<Object> nils;
    private Node[] table;

    public CinqMultimap() {
        table = new Node[16];
        entries = 0;
    }

    static int hash(Object key) {
        if (key == null) return 0;
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    static boolean eq(Object key1, Object key2) {
        if (key1 == null) return false;
        if (key2 == null) return false;
        return key1.equals(key2);
        // much slower using equiv - consider fast option for common cases.
        //return CinqUtil.eq(key1, key2);
    }

    private static class Node {
        public int hash;
        public Object key;
        public List<Object> value;

        public Node(int h, Object key, List<Object> value) {
            this.hash = h;
            this.key = key;
            this.value = value;
        }
    }

    public int size() {
        return entries + (nils == null ? 0 : 1);
    }

    private void insertNode(int i, int h, Object key, Object value) {
        List<Object> al = new ArrayList<>();
        al.add(value);
        Node newNode = new Node(h, key, al);
        table[i] = newNode;
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
            if (nils == null) {
                nils = new ArrayList<>();
            }
            nils.add(value);
        }

        if ((entries * 2) > table.length) {
            resize();
        }

        int m = (table.length - 1);
        int i = h & m;

        while (true) {
            Node node = table[i];
            if (node == null) {
                insertNode(i, h, key, value);
                return;
            } else if (node.hash == h && eq(node.key, key)) {
                node.value.add(value);
                return;
            } else if (psl(h, i, m) > psl(node.hash, i, m)) {
                insertNode(i, h, key, value);
                continueNode(i + 1, node);
                return;
            }
            i = (i + 1) & m;
        }
    }

    private void continueNode(int i, Node contNode) {
        int m = (table.length - 1);
        i = i & m;
        while (true) {
            Node node = table[i];
            if (node == null) {
                table[i] = contNode;
                return;
            }
            if (psl(contNode.hash, i, m) > psl(node.hash, i, m)) {
                table[i] = contNode;
                contNode = node;
            }
            i = (i + 1) & m;
        }
    }

    private void resize() {
        Node[] oldTable = table;
        table = new Node[oldTable.length * 2];

        for (int i = 0; i < oldTable.length; i++) {
            Node n = oldTable[i];
            if (n != null) {
                continueNode(n.hash & (table.length - 1), n);
            }
        }
    }

    public List<Object> get(Object key) {
        if (key == null) {
            return nils;
        }
        int h = hash(key);
        int m = (table.length - 1);
        int i = h & m;
        Node node;
        while ((node = table[i]) != null) {

            if (node.hash == h && eq(node.key, key)) {
                return node.value;
            }
            else if (psl(h, i, m) > psl(node.hash, i, m)) {
                return null;
            }

            i = (i + 1) & m;
        }
        return null;
    }

    public void put(Object key, Object tuple) {
        doPutTuple(hash(key), key, tuple);
    }

    public void forEach(Consumer<Object> f) {
        if (nils != null) {
            nils.forEach(f);
        }

        for (int i = 0; i < table.length; i++) {
            Node n = table[i];

            if (n == null) {
                continue;
            }

            n.value.forEach(f);
        }
    }

    public void forEachEntry(BiConsumer<Object, List<Object>> f) {
        if (nils != null) {
            f.accept(null, nils);
        }

        for (int i = 0; i < table.length; i++) {
            Node n = table[i];

            if (n == null) {
                continue;
            }
            f.accept(n.key, n.value);
        }
    }
}
