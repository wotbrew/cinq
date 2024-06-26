package com.wotbrew.cinq;

import clojure.lang.IFn;

import java.nio.ByteBuffer;

public interface CinqScanFunction extends IFn {

    Object apply(Object acc, long rsn, Object x);

    // implement both of these if you have scan filters
    default boolean nativeFilter(long rsn, ByteBuffer val) { return true ; }
    default boolean filter(long rsn, Object x) { return true ; }

    // when used in IReduceInit we implicitly apply the filter
    @Override
    default Object invoke(Object arg1, Object arg2) {
        if (filter(-1, arg2)) {
            return apply(arg1, -1, arg2);
        }
        return arg1;
    }

    @Override
    default Object invoke(Object arg1, Object arg2, Object arg3) {
        long rsn = (long) arg2;
        if (filter(rsn, arg3)) {
            return apply(arg1, rsn, arg3);
        }
        return arg1;
    }
}
