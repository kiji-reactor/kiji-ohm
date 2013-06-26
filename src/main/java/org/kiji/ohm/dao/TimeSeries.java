package org.kiji.ohm.dao;

import java.util.Map;
import java.util.TreeMap;

import org.kiji.schema.util.TimestampComparator;

public class TimeSeries<V> extends TreeMap<Long, V> {

  public TimeSeries() {
    super(TimestampComparator.INSTANCE);
  }

  public static <V> TimeSeries<V> fromMap(Map<Long, V> map) {
    final TimeSeries<V> timeseries = new TimeSeries<V>();
    timeseries.putAll(map);
    return timeseries;
  }
}
