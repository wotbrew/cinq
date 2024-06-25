package com.wotbrew.cinq;

import clojure.java.api.Clojure;
import clojure.lang.*;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CinqDynamicArrayMap extends APersistentMap {
    private final Object[] keys;
    private final Object[] vals;
    private final int[] offsets;
    private final ByteBuffer buffer;
    private final Object[] symbolList;
    private final IFn decodeInstance;

    private final static Var decode = (Var)Clojure.var("com.wotbrew.cinq.nio-codec/decode-object");

    public static CinqDynamicArrayMap read(ByteBuffer buffer, Object[] symbolList) {
        int len = buffer.getInt();
        Object[] keys = new Object[len];
        Object[] vals = new Object[len];
        int[] offsets = new int[len];

        IFn decodeInstance = (IFn)decode.getRawRoot();

        decodeOffsets(buffer, len, offsets);
        decodeKeys(buffer, symbolList, len, keys, decodeInstance);
        ByteBuffer heapBuffer = ensureHeapSlice(buffer);

        return new CinqDynamicArrayMap(keys, vals, offsets, heapBuffer, symbolList, decodeInstance);
    }

    private static ByteBuffer ensureHeapSlice(ByteBuffer buffer) {
        ByteBuffer heapBuffer;
        if (buffer.isDirect()) {
            heapBuffer = ByteBuffer.allocate(buffer.remaining());
            heapBuffer.put(buffer);
            heapBuffer.flip();
        } else {
            heapBuffer = buffer.slice();
        }
        return heapBuffer;
    }

    private static void decodeOffsets(ByteBuffer buffer, int len, int[] offsets) {
        for (int i = 0; i < len; i++) {
            offsets[i] = buffer.getInt();
        }
    }

    private static void decodeKeys(ByteBuffer buffer, Object[] symbolList, int len, Object[] keys, IFn decodeInstance) {
        for (int i = 0; i < len; i++) {
            keys[i] = decodeInstance.invoke(buffer, symbolList);
        }
    }

    private CinqDynamicArrayMap(Object[] keys,
                                Object[] vals,
                                int[] offsets,
                                ByteBuffer buffer,
                                Object[] symbols,
                                IFn decodeInstance) {
        this.keys = keys;
        this.vals = vals;
        this.offsets = offsets;
        this.buffer = buffer;
        this.symbolList = symbols;
        this.decodeInstance = decodeInstance;
    }

    private int indexOfObject(Object key) {
        Util.EquivPred ep = Util.equivPred(key);
        for (int i = 0; i < keys.length; i++) {
            if (ep.equiv(key, keys[i]))
                return i;
        }
        return -1;
    }

    private int indexOf(Object key) {
        if (key instanceof Keyword) {
            for (int i = 0; i < keys.length; i++) {
                if (key == keys[i])
                    return i;
            }
            return -1;
        } else
            return indexOfObject(key);
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }

    private Object valAtIndex(int i) {
        Object v = vals[i];
        if (v != null) return v;
        int offset = offsets[i];
        int pos = buffer.position();
        buffer.position(offset);
        v = decodeInstance.invoke(buffer, symbolList);
        buffer.position(pos);
        vals[i] = v;
        return v;
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        int i = indexOf(key);
        if (i >= 0) {
            return valAtIndex(i);
        }
        return notFound;
    }

    @Override
    public boolean containsKey(Object key) {
        return indexOf(key) >= 0;
    }

    @Override
    public IMapEntry entryAt(Object key) {
        Object notFound = new Object();
        Object val = valAt(key, notFound);
        if (val == notFound) {
            return null;
        }
        return getMapEntry(val, key);
    }

    private IMapEntry entryAtIndex(int i) {
        Object val = valAtIndex(i);
        Object key = keys[i];
        return getMapEntry(val, key);
    }

    private IMapEntry getMapEntry(Object val, Object key) {
        return new IMapEntry() {
            @Override
            public Object key() {
                return key;
            }

            @Override
            public Object val() {
                return val;
            }

            @Override
            public Object getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                return val;
            }

            @Override
            public Object setValue(Object value) {
                throw new RuntimeException("Not supported (setValue)");
            }
        };
    }

    @Override
    public IPersistentMap assoc(Object key, Object val) {
        ITransientMap t = PersistentArrayMap.EMPTY.asTransient();
        for (int i = 0; i < keys.length; i++) {
            t = t.assoc(keys[i], valAtIndex(i));
        }
        t = t.assoc(key, val);
        return t.persistent();
    }

    @Override
    public IPersistentMap assocEx(Object key, Object val) {
        ITransientMap t = PersistentArrayMap.EMPTY.asTransient();
        for (int i = 0; i < keys.length; i++) {
            t = t.assoc(keys[i], valAtIndex(i));
        }
        IPersistentMap m = t.persistent();
        return m.assocEx(key, val);
    }

    @Override
    public IPersistentMap without(Object key) {
        ITransientMap t = PersistentArrayMap.EMPTY.asTransient();
        for (int i = 0; i < keys.length; i++) {
            t = t.assoc(keys[i], valAtIndex(i));
        }
        t = t.without(key);
        return t.persistent();
    }

    @Override
    public int count() {
        return keys.length;
    }

    @Override
    public IPersistentCollection empty() {
        return PersistentArrayMap.EMPTY;
    }

    @Override
    public ISeq seq() {
        if (keys.length > 0)
            return new Seq(this, 0);
        return null;
    }

    static class Seq extends ASeq implements Counted {
        final CinqDynamicArrayMap m;
        final int i;

        Seq(CinqDynamicArrayMap m, int i) {
            this.m = m;
            this.i = i;
        }

        Seq(IPersistentMap meta, CinqDynamicArrayMap m, int i) {
            super(meta);
            this.m = m;
            this.i = i;
        }

        public Object first() {
            return MapEntry.create(m.keys[i], m.valAtIndex(i));
        }

        public ISeq next() {
            if (i + 1 < m.keys.length)
                return new Seq(m, i + 1);
            return null;
        }

        public int count() {
            return m.keys.length - i;
        }

        public Obj withMeta(IPersistentMap meta) {
            if (meta() == meta)
                return this;
            return new Seq(meta, m, i);
        }
    }

    static class Iter implements Iterator {
        CinqDynamicArrayMap m;
        int i;

        //for iterator
        Iter(CinqDynamicArrayMap m) {
            this.m = m;
        }

        public boolean hasNext() {
            return i < m.keys.length;
        }

        public Object next() {
            try {
                Object o = m.entryAtIndex(i);
                i++;
                return o;
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public Iterator iterator() {
        return new Iter(this);
    }
}
