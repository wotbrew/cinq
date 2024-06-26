package com.wotbrew.cinq;

import clojure.lang.IFn;

import java.nio.ByteBuffer;

public interface CinqScanFunction extends IFn {

    Object apply(Object acc, long rsn, Object x);

    // implement both of these if you have scan filters
    boolean nativeFilter(long rsn, ByteBuffer val);
    boolean filter(long rsn, Object x);

    // when used in IReduceInit we implicitly apply the filter
    @Override
    Object invoke(Object arg1, Object arg2);

    @Override
    Object invoke(Object arg1, Object arg2, Object arg3);
}
