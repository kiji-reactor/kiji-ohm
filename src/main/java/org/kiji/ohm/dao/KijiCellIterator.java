package org.kiji.ohm.dao;

import java.util.Iterator;

import org.kiji.schema.KijiCell;

public interface KijiCellIterator<V> extends Iterator<KijiCell<V>> {
}
