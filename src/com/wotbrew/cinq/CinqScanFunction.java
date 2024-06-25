package com.wotbrew.cinq;

import clojure.lang.IFn;

public interface CinqScanFunction extends IFn {
    Object apply(Object acc, long rsn, Object x);

    @Override
    default Object invoke(Object arg1, Object arg2) {
        return apply(arg1, -1, arg2);
    }

    @Override
    default Object invoke(Object arg1, Object arg2, Object arg3) {
        return apply(arg1, (Long) arg2, arg3);
    }
}
