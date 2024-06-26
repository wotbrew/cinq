package com.wotbrew.cinq;

import clojure.lang.IFn;

import java.nio.ByteBuffer;

public interface CinqScanFunction extends IFn {

    boolean rootDoesNotEscape();

    Object apply(Object acc, long rsn, Object x);

    interface NativeFilter {
        boolean apply(long rsn, ByteBuffer x);
    }

    NativeFilter nativeFilter(Object symbolTable);

    // implement both of these if you have scan filters
    boolean filter(long rsn, Object x);

    // when used in IReduceInit we implicitly apply the filter
    @Override
    Object invoke(Object arg1, Object arg2);

    @Override
    Object invoke(Object arg1, Object arg2, Object arg3);
}
