package org.kiji.ohm.dao;

import java.util.NavigableMap;

public class MapTypeValue<V> extends AbstractDelegateNavigableMap<String, V> {
  private NavigableMap<String, V> mQualifierMap;

  public MapTypeValue(NavigableMap<String, V> qualifierMap) {
    super(qualifierMap);
  }
}
