package com.wotbrew.cinq;

import clojure.java.api.Clojure;
import clojure.lang.*;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class CinqUnsafeDynamicMap implements ILookup {

    private Object[] keys;
    private int[] offsets;
    private int length;
    private ByteBuffer buffer;
    private final Object[] symbolList;
    private final IFn decodeInstance;
    int startPos;

    private final static Var decode = (Var) Clojure.var("com.wotbrew.cinq.nio-codec/decode-object");

    public CinqUnsafeDynamicMap(Object[] symbolList) {
        this.keys = new Object[16];
        this.offsets = new int[16];
        this.length = 0;
        this.symbolList = symbolList;
        this.decodeInstance = (IFn) decode.getRawRoot();
    }

    public void read(ByteBuffer buffer) {
        this.length = buffer.getInt();
        // keysize
        buffer.getInt();
        this.buffer = buffer;
        resizeIfNeeded();
        readOffsets();
        readKeys();
        startPos = this.buffer.position();
    }

    private void readKeys() {
        for (int i = 0; i < length; i++) {
            keys[i] = decodeInstance.invoke(buffer, symbolList);
        }
    }

    private void readOffsets() {
        for (int i = 0; i < length; i++) {
            offsets[i] = buffer.getInt();
        }
    }

    private void resizeIfNeeded() {
        while (keys.length < length) {
            keys = new Object[keys.length * 2];
            offsets = new int[offsets.length * 2];
        }
    }
    private int indexOfObject(Object key) {
        Util.EquivPred ep = Util.equivPred(key);
        for (int i = 0; i < length; i++) {
            if (ep.equiv(key, keys[i]))
                return i;
        }
        return -1;
    }

    private int indexOf(Object key) {
        if (key instanceof Keyword) {
            for (int i = 0; i < length; i++) {
                if (key == keys[i])
                    return i;
            }
            return -1;
        } else
            return indexOfObject(key);
    }


    @Override
    public Object valAt(Object key, Object notFound) {
        int i = indexOf(key);
        if (i == -1) {
            return notFound;
        }
        int offset = offsets[i];
        buffer.position(startPos + offset);
        return decodeInstance.invoke(buffer, symbolList);
    }

    @Override
    public Object valAt(Object key) {
        return valAt(key, null);
    }
}
