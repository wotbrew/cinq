package com.wotbrew.cinq;

import clojure.lang.Murmur3;
import clojure.lang.Util;

import java.util.ArrayList;
import java.util.HashMap;

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
}
