package org.kiji.ohm.dao;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

public abstract class AbstractDelegateNavigableMap<K, V> implements NavigableMap<K, V> {

  private NavigableMap<K, V> mDelegateMap;

  public AbstractDelegateNavigableMap(NavigableMap<K, V> delegateMap) {
    mDelegateMap = delegateMap;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key) {
    return mDelegateMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return mDelegateMap.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return mDelegateMap.get(key);
  }

  @Override
  public boolean isEmpty() {
    return mDelegateMap.isEmpty();
  }

  @Override
  public V put(K key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return mDelegateMap.size();
  }

  @Override
  public Comparator<? super K> comparator() {
    return mDelegateMap.comparator();
  }

  @Override
  public Set<java.util.Map.Entry<K, V>> entrySet() {
    return mDelegateMap.entrySet();
  }

  @Override
  public K firstKey() {
    return mDelegateMap.firstKey();
  }

  @Override
  public Set<K> keySet() {
    return mDelegateMap.keySet();
  }

  @Override
  public K lastKey() {
    return mDelegateMap.lastKey();
  }

  @Override
  public Collection<V> values() {
    return mDelegateMap.values();
  }

  @Override
  public java.util.Map.Entry<K, V> ceilingEntry(K key) {
    return mDelegateMap.ceilingEntry(key);
  }

  @Override
  public K ceilingKey(K key) {
    return mDelegateMap.ceilingKey(key);
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    return mDelegateMap.descendingKeySet();
  }

  @Override
  public NavigableMap<K, V> descendingMap() {
    return mDelegateMap.descendingMap();
  }

  @Override
  public java.util.Map.Entry<K, V> firstEntry() {
    return mDelegateMap.firstEntry();
  }

  @Override
  public java.util.Map.Entry<K, V> floorEntry(K key) {
    return mDelegateMap.floorEntry(key);
  }

  @Override
  public K floorKey(K key) {
    return mDelegateMap.floorKey(key);
  }

  @Override
  public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return mDelegateMap.headMap(toKey, inclusive);
  }

  @Override
  public SortedMap<K, V> headMap(K toKey) {
    return mDelegateMap.headMap(toKey);
  }

  @Override
  public java.util.Map.Entry<K, V> higherEntry(K key) {
    return mDelegateMap.higherEntry(key);
  }

  @Override
  public K higherKey(K key) {
    return mDelegateMap.higherKey(key);
  }

  @Override
  public java.util.Map.Entry<K, V> lastEntry() {
    return mDelegateMap.lastEntry();
  }

  @Override
  public java.util.Map.Entry<K, V> lowerEntry(K key) {
    return mDelegateMap.lowerEntry(key);
  }

  @Override
  public K lowerKey(K key) {
    return mDelegateMap.lowerKey(key);
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    return mDelegateMap.navigableKeySet();
  }

  @Override
  public java.util.Map.Entry<K, V> pollFirstEntry() {
    return mDelegateMap.pollFirstEntry();
  }

  @Override
  public java.util.Map.Entry<K, V> pollLastEntry() {
    return mDelegateMap.pollLastEntry();
  }

  @Override
  public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return mDelegateMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public SortedMap<K, V> subMap(K fromKey, K toKey) {
    return mDelegateMap.subMap(fromKey, toKey);
  }

  @Override
  public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return mDelegateMap.tailMap(fromKey, inclusive);
  }

  @Override
  public SortedMap<K, V> tailMap(K fromKey) {
    // TODO Auto-generated method stub
    return mDelegateMap.tailMap(fromKey);
  }

}
