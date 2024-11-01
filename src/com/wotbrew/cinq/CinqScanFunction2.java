package com.wotbrew.cinq;
import java.nio.ByteBuffer;

public interface CinqScanFunction2 {
    boolean rootDoesNotEscape();
    Object apply(Object acc, Object relvar, Object pk, Object x);
    interface NativeFilter {
        Boolean fwd();
        ByteBuffer startKey();
        Boolean indexKeyCont(ByteBuffer key);
        Boolean indexKeyPred(ByteBuffer key);
        Boolean valPred(ByteBuffer val);
    }
    NativeFilter nativeFilter(Object symbolTable);
}
