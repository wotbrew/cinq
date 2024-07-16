package com.wotbrew.cinq;

import java.nio.ByteBuffer;

public interface CinqScanFunction {

    boolean rootDoesNotEscape();

    Object apply(Object acc, Object relvar, long rsn, Object x);

    interface NativeFilter {
        Boolean apply(long rsn, ByteBuffer x);
    }

    NativeFilter nativeFilter(Object symbolTable);

    // implement both of these if you have scan filters
    Boolean filter(long rsn, Object x);

}
