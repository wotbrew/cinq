package com.wotbrew.cinq;

import clojure.lang.Keyword;
import clojure.lang.Murmur3;
import clojure.lang.Util;

public class CinqUtil {

  public static boolean equalKey(Object k1, Object k2){
    if(k1 instanceof Keyword)
      return k1 == k2;
    return Util.equiv(k1, k2);
  }

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

  public static Object minStep(Object acc, Object n) {
    if (n == null) return acc;
    if (acc == null) return n;
    if (Util.compare(acc, n) < 0) {
      return acc;
    }
    return n;
  }

  public static Object maxStep(Object acc, Object n) {
    if (n == null) return acc;
    if (acc == null) return n;
    if (Util.compare(acc, n) > 0) {
      return acc;
    }
    return n;
  }

  public static Object add(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static Object add(long a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static Object add(Object a, long b) {
    if (a == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static Object add(double a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static Object add(Object a, double b) {
    if (a == null) return null;
    return clojure.lang.Numbers.add(a, b);
  }
  public static long add(long a, long b) {
    return clojure.lang.Numbers.add(a, b);
  }
  public static double add(double a, double b) {
    return clojure.lang.Numbers.add(a, b);
  }
  public static double add(long a, double b) {
    return clojure.lang.Numbers.add(a, b);
  }
  public static double add(double a, long b) {
    return clojure.lang.Numbers.add(a, b);
  }

  public static Object sumStep(Object acc, Object n) {
    if (n == null) return acc;
    if (acc == null) return n;
    return clojure.lang.Numbers.add(acc, n);
  }
  public static Object sumStep(long acc, Object n) {
    if (n == null) return acc;
    return clojure.lang.Numbers.add(acc, n);
  }
  public static Object sumStep(Object acc, long n) {
    if (acc == null) return n;
    return clojure.lang.Numbers.add(acc, n);
  }
  public static Object sumStep(double acc, Object n) {
    if (n == null) return acc;
    return clojure.lang.Numbers.add(acc, n);
  }
  public static Object sumStep(Object acc, double n) {
    if (acc == null) return n;
    return clojure.lang.Numbers.add(acc, n);
  }

  public static long sumStep(long acc, long n) {
    return clojure.lang.Numbers.add(acc, n);
  }
  public static double sumStep(double acc, double n) {
    return clojure.lang.Numbers.add(acc, n);
  }
  public static double sumStep(double acc, long n) {
    return clojure.lang.Numbers.add(acc, n);
  }
  public static double sumStep(long acc, double n) {
    return clojure.lang.Numbers.add(acc, n);
  }

  public static Object sub(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static Object sub(long a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static Object sub(Object a, long b) {
    if (a == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static Object sub(double a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static Object sub(Object a, double b) {
    if (a == null) return null;
    return clojure.lang.Numbers.minus(a, b);
  }
  public static long sub(long a, long b) {
    return clojure.lang.Numbers.minus(a, b);
  }
  public static double sub(double a, double b) {
    return clojure.lang.Numbers.minus(a, b);
  }
  public static double sub(long a, double b) {
    return clojure.lang.Numbers.minus(a, b);
  }
  public static double sub(double a, long b) {
    return clojure.lang.Numbers.minus(a, b);
  }

  public static Object mul(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static Object mul(long a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static Object mul(Object a, long b) {
    if (a == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static Object mul(double a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static Object mul(Object a, double b) {
    if (a == null) return null;
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static long mul(long a, long b) {
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static double mul(double a, double b) {
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static double mul(long a, double b) {
    return clojure.lang.Numbers.multiply(a, b);
  }
  public static double mul(double a, long b) {
    return clojure.lang.Numbers.multiply(a, b);
  }

  public static Object div(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
  public static Object div(long a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
  public static Object div(Object a, long b) {
    if (a == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
  public static Object div(double a, Object b) {
    if (b == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
  public static Object div(Object a, double b) {
    if (a == null) return null;
    return clojure.lang.Numbers.divide(a, b);
  }
  public static long div(long a, long b) {
    return clojure.lang.Numbers.quotient(a, b);
  }
  public static double div(double a, double b) {
    return clojure.lang.Numbers.divide(a, b);
  }
  public static double div(long a, double b) {
    return clojure.lang.Numbers.divide(a, b);
  }
  public static double div(double a, long b) {
    return clojure.lang.Numbers.divide(a, b);
  }

  // null != null, null != any-obj
  public static Boolean eq(Object a, Object b) {
    if (a == null) return null;
    if (b == null) return null;
    // equiv very slow - investigate options
    if(a.equals(b)) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(long a, Object b) {
    if (b == null) return null;
    // equiv very slow - investigate options
    if(b.equals(a)) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(Object a, long b) {
    if (a == null) return null;
    // equiv very slow - investigate options
    if(a.equals(b)) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(double a, Object b) {
    if (b == null) return null;
    // equiv very slow - investigate options
    if(b.equals(a)) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(Object a, double b) {
    if (a == null) return null;
    // equiv very slow - investigate options
    if(a.equals(b)) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(long a, long b) {
    if(a == b) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(double a, double b) {
    if(a == b) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(double a, long b) {
    if(a == b) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean eq(long a, double b) {
    if(a == b) return Boolean.TRUE;
    return Boolean.FALSE;
  }

  public static Boolean lt(long a, long b) {
    return a < b;
  }
  public static Boolean lt(double a, double b) {
    return a < b;
  }
  public static Boolean lt(double a, long b) {
    return Util.compare(a, b) < 0;
  }
  public static Boolean lt(long a, double b) {
    return Util.compare(a, b) < 0;
  }
  public static Boolean lt(Object a, Object b) {
    if(a == null) return null;
    if(b == null) return null;
    if(Util.compare(a, b) < 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lt(Object a, long b) {
    if(a == null) return null;
    if(Util.compare(a, b) < 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lt(long a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) < 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lt(double a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) < 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lt(Object a, double b) {
    if(a == null) return null;
    if(Util.compare(a, b) < 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }

  public static Boolean lte(long a, long b) {
    return a <= b;
  }
  public static Boolean lte(double a, double b) {
    return a <= b;
  }
  public static Boolean lte(double a, long b) {
    return Util.compare(a, b) <= 0;
  }
  public static Boolean lte(long a, double b) {
    return Util.compare(a, b) <= 0;
  }
  public static Boolean lte(Object a, Object b) {
    if(a == null) return null;
    if(b == null) return null;
    if(Util.compare(a, b) <= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lte(Object a, long b) {
    if(a == null) return null;
    if(Util.compare(a, b) <= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lte(long a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) <= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lte(double a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) <= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean lte(Object a, double b) {
    if(a == null) return null;
    if(Util.compare(a, b) <= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  
  public static Boolean gt(long a, long b) {
    return a > b;
  }
  public static Boolean gt(double a, double b) {
    return a > b;
  }
  public static Boolean gt(double a, long b) {
    return Util.compare(a, b) > 0;
  }
  public static Boolean gt(long a, double b) {
    return Util.compare(a, b) > 0;
  }
  public static Boolean gt(Object a, Object b) {
    if(a == null) return null;
    if(b == null) return null;
    if(Util.compare(a, b) > 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gt(Object a, long b) {
    if(a == null) return null;
    if(Util.compare(a, b) > 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gt(long a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) > 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gt(double a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) > 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gt(Object a, double b) {
    if(a == null) return null;
    if(Util.compare(a, b) > 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }

  public static Boolean gte(long a, long b) {
    return a >= b;
  }
  public static Boolean gte(double a, double b) {
    return a >= b;
  }
  public static Boolean gte(double a, long b) {
    return Util.compare(a, b) >= 0;
  }
  public static Boolean gte(long a, double b) {
    return Util.compare(a, b) >= 0;
  }
  public static Boolean gte(Object a, Object b) {
    if(a == null) return null;
    if(b == null) return null;
    if(Util.compare(a, b) >= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gte(Object a, long b) {
    if(a == null) return null;
    if(Util.compare(a, b) >= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gte(long a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) >= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gte(double a, Object b) {
    if(b == null) return null;
    if(Util.compare(a, b) >= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
  public static Boolean gte(Object a, double b) {
    if(a == null) return null;
    if(Util.compare(a, b) >= 0) return Boolean.TRUE;
    return Boolean.FALSE;
  }
}
