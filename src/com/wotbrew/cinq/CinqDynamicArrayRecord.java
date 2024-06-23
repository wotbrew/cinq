package com.wotbrew.cinq;

import clojure.java.api.Clojure;
import clojure.lang.*;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class CinqDynamicArrayRecord extends APersistentMap {
    Object[] keys;
    Object[] vals;
    int[] offsets;
    ByteBuffer buffer;
    Object symbolList;

    static IFn decode = Clojure.var("com.wotbrew.cinq.nio-codec/decode-object");

    public CinqDynamicArrayRecord(Object[] keys, Object[] vals,
                                  int[] offsets,
                                  ByteBuffer buffer,
                                  Object symbols)
    {
        this.keys = keys;
        this.vals = vals;
        this.offsets = offsets;
        this.buffer = buffer;
        this.symbolList = symbols;
    }

    private int indexOfObject(Object key){
        Util.EquivPred ep = Util.equivPred(key);
        for(int i = 0; i < keys.length; i++)
        {
            if(ep.equiv(key, keys[i]))
                return i;
        }
        return -1;
    }

    private int indexOf(Object key){
        if(key instanceof Keyword)
        {
            for(int i = 0; i < keys.length; i++)
            {
                if(key == keys[i])
                    return i;
            }
            return -1;
        }
        else
            return indexOfObject(key);
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }

    private Object valAtIndex(int i) {
        Object v = vals[i];
        if(v != null) return v;
        int offset = offsets[i];
        int pos = buffer.position();
        buffer.position(offset);
        v = decode.invoke(buffer, symbolList);
        buffer.position(pos);
        vals[i] = v;
        return v;
    }

    @Override
    public Object valAt(Object key, Object notFound) {
        int i = indexOf(key);
        if(i >= 0) {
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
        for(int i = 0; i < keys.length; i++) {
            t = t.assoc(keys[i], valAtIndex(i));
        }
        t = t.assoc(key, val);
        return t.persistent();
    }

    @Override
    public IPersistentMap assocEx(Object key, Object val) {
        ITransientMap t = PersistentArrayMap.EMPTY.asTransient();
        for(int i = 0; i < keys.length; i++) {
            t = t.assoc(keys[i], valAtIndex(i));
        }
        IPersistentMap m = t.persistent();
        return m.assocEx(key, val);
    }

    @Override
    public IPersistentMap without(Object key) {
        ITransientMap t = PersistentArrayMap.EMPTY.asTransient();
        for(int i = 0; i < keys.length; i++) {
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
        if(keys.length > 0)
            return new Seq(this, 0);
        return null;
    }

    static class Seq extends ASeq implements Counted{
        final CinqDynamicArrayRecord m;
        final int i;

        Seq(CinqDynamicArrayRecord m, int i){
            this.m = m;
            this.i = i;
        }

        Seq(IPersistentMap meta, CinqDynamicArrayRecord m, int i){
            super(meta);
            this.m = m;
            this.i = i;
        }

        public Object first(){
            return MapEntry.create(m.keys[i], m.valAtIndex(i));
        }

        public ISeq next(){
            if(i + 1 < m.keys.length)
                return new Seq(m, i + 1);
            return null;
        }

        public int count(){
            return m.keys.length - i;
        }

        public Obj withMeta(IPersistentMap meta){
            if(meta() == meta)
                return this;
            return new Seq(meta, m, i);
        }
    }

    static class Iter implements Iterator {
        CinqDynamicArrayRecord m;
        int i;

        //for iterator
        Iter(CinqDynamicArrayRecord m){
            this.m = m;
        }

        public boolean hasNext(){
            return i < m.keys.length;
        }

        public Object next(){
            try {
                Object o = m.entryAtIndex(i);
                i++;
                return o;
            } catch(IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        public void remove(){
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public Iterator iterator() {
        return new Iter(this);
    }
}
