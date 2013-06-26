package org.kiji.ohm.dao;

public class TCell<T> {

  private T mValue;
  private long mTimestamp;

  public TCell(long timestamp, T value) {
    mTimestamp = timestamp;
    mValue = value;
  }

  public T getValue() {
    return mValue;
  }

  public long getTimestamp() {
    return mTimestamp;
  }

}
