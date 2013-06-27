package org.kiji.ohm.dao;

public abstract class AbstractKijiCellIterator<V> implements KijiCellIterator<V> {
  /** {@inheritDoc} */
  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
