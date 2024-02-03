package com.wotbrew.cinq;

import java.util.Iterator;

public class ArrayIterator implements Iterator {

  int i;
  Object[] a;

  public ArrayIterator(Object[] a) {
    this.a = a;
  }

  @Override
  public boolean hasNext() {
    return i < a.length;
  }

  @Override
  public Object next() {
    return a[i++];
  }
}
