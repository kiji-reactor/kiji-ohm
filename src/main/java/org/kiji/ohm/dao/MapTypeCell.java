package org.kiji.ohm.dao;

import java.util.NavigableMap;

import org.kiji.schema.KijiCell;

public class MapTypeCell<V> extends AbstractDelegateNavigableMap<String, KijiCell<V>> {

  public MapTypeCell(NavigableMap<String, KijiCell<V>> qualifierMap) {
    super(qualifierMap);
  }
}
