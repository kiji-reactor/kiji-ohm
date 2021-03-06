package org.kiji.ohm.dao;

import java.util.Iterator;

import org.kiji.schema.KijiCell;

public class KijiCellIterator<V> implements Iterator<KijiCell<V>> {
  private Iterator<KijiCell<V>> mKijiCellIterator;

  public KijiCellIterator(Iterator<KijiCell<V>> kijiCellIterator) {
    mKijiCellIterator = kijiCellIterator;
  }

  @Override
  public boolean hasNext() {
    // TODO Auto-generated method stub
    return mKijiCellIterator.hasNext();
  }

  @Override
  public KijiCell<V> next() {
    // TODO Auto-generated method stub
    return mKijiCellIterator.next();
  }

  @Override
  public void remove() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }


}
