package com.wotbrew.cinq;

import clojure.lang.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class CinqMultimap {
    // linear probing multi-map for now

    int entries;
    List<Object> nils;

    private Node[] table;

    public CinqMultimap() {
        table = new Node[16];
        entries = 0;
    }

    static int hash(Object key) {
        return Util.hash(key);
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

        int i = h % table.length;
        while (true) {
            Node node = table[i];
            if (node == null) {
                List<Object> al = new ArrayList<>();
                al.add(value);
                Node newNode = new Node(h, key, al);
                table[i] = newNode;
                entries++;
                return;
            }
            else if (node.hash == h && eq(node.key, key)) {
                node.value.add(value);
                return;
            }
            i = (i + 1) % table.length;
        }
    }

    private void resize() {
        Node[] oldTable = table;
        table = new Node[oldTable.length * 2];

        for (int i = 0; i < oldTable.length; i++) {
            Node n = oldTable[i];
            if(n != null) {
                int j = n.hash % table.length;;
                while (true) {
                    Node node = table[j];
                    if (node == null) {
                        table[j] = n;
                        break;
                    }
                    j = (j + 1) % table.length;
                }
            }
        }
    }

    public List<Object> get(Object key) {
        if (key == null) {
            return nils;
        }
        int h = hash(key);
        int i = h % table.length;
        Node node;
        while ((node = table[i]) != null) {
            if (node.hash == h && eq(node.key, key)) {
                return node.value;
            }
            i = (i + 1) % table.length;
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

            if(n == null) {
                continue;
            }

            n.value.forEach(f);
        }
    }
}
