package com.wotbrew.cinq;

import clojure.lang.Murmur3;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.BiFunction;

public class CinqUtil {

  public static int hasheqArray(Object[] arr) {
    int h = 1;
    for (int i = 0; i < arr.length; i++) {
      h = 31 * h + clojure.lang.Util.hasheq(arr[i]);
    }
    return Murmur3.mixCollHash(h, arr.length);
  }

  public static int hashArray(Object[] arr) {
    int h = 1;
    for (int i = 0; i < arr.length; i++) {
      h = 31 * h + clojure.lang.Util.hash(arr[i]);
    }
    return h;
  }

  public static void htCompute(HashMap m, Object k, Object t) {
    m.compute(k, (o, o2) -> {
      ArrayList l = (ArrayList) o2;
      if (l == null) {
        l = new ArrayList();
      }
      l.add(t);
      return l;
    });
  }

  public static Object add(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static Object sub(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static Object mul(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static Object div(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
}
