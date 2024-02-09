package com.wotbrew.cinq;

import clojure.lang.Murmur3;

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

}
