package org.kiji.ohm.dao;

import java.util.TreeMap;

import org.kiji.schema.util.TimestampComparator;

public class TimeSeries<V> extends TreeMap<Long, V>{

  public TimeSeries() {
    super(TimestampComparator.INSTANCE);
  }
}
