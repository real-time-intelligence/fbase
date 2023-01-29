package org.fbase.util;

import java.util.LinkedHashMap;

public class CachedLastLinkedHashMap<K,V> extends LinkedHashMap<K, V> {
  private V last = null;

  @Override
  public V put(K key, V value) {
    last = value;
    return super.put(key, value);
  }

  public V getLast() {
    return last;
  }
}
