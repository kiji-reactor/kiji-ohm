package org.kiji.ohm.dao;

import java.util.Iterator;

import org.kiji.schema.KijiCell;

public class KijiCellValueIterator<V> implements Iterator<V> {

  private Iterator<KijiCell<V>> mKijiCellIterator;

  public KijiCellValueIterator(Iterator<KijiCell<V>> kijiCellIterator) {
    mKijiCellIterator = kijiCellIterator;
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return mKijiCellIterator.hasNext();
  }

  @Override
  public V next() {
    // TODO Auto-generated method stub
    return mKijiCellIterator.next().getData();
  }

  @Override
  public void remove() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
}
