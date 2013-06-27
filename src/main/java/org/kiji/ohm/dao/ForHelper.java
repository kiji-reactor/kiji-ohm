package org.kiji.ohm.dao;

import java.util.Iterator;

public final class ForHelper {
  private ForHelper() {
  }

  /**
   * Helper to build for loops around naked iterators.
   *
   * <p> <tt><pre>{@code
   *   for (T value : ForHelper.from(iterator)) {
   *     ...
   *   }
   * } </pre></tt> </p>
   *
   * @param iterator
   * @return
   */
  public static <T> Iterable<T> from(final Iterator<T> iterator) {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator() {
        return iterator;
      }
    };
  }
}
