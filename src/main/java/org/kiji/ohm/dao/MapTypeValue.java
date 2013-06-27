package org.kiji.ohm.dao;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;

public class MapTypeValue<V> implements NavigableMap<String, V> {
  private NavigableMap<String, V> mQualifierMap;

  public MapTypeValue(NavigableMap<String, V> qualifierMap) {
    mQualifierMap = qualifierMap;
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key) {
    return mQualifierMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return mQualifierMap.containsValue(value);
  }

  @Override
  public V get(Object key) {
    return mQualifierMap.get(key);
  }

  @Override
  public boolean isEmpty() {
    return mQualifierMap.isEmpty();
  }

  @Override
  public V put(String key, V value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    return mQualifierMap.size();
  }

  @Override
  public Comparator<? super String> comparator() {
    return mQualifierMap.comparator();
  }

  @Override
  public Set<java.util.Map.Entry<String, V>> entrySet() {
    return mQualifierMap.entrySet();
  }

  @Override
  public String firstKey() {
    return mQualifierMap.firstKey();
  }

  @Override
  public Set<String> keySet() {
    return mQualifierMap.keySet();
  }

  @Override
  public String lastKey() {
    return mQualifierMap.lastKey();
  }

  @Override
  public Collection<V> values() {
    return mQualifierMap.values();
  }

  @Override
  public java.util.Map.Entry<String, V> ceilingEntry(String key) {
    return mQualifierMap.ceilingEntry(key);
  }

  @Override
  public String ceilingKey(String key) {
    return mQualifierMap.ceilingKey(key);
  }

  @Override
  public NavigableSet<String> descendingKeySet() {
    return mQualifierMap.descendingKeySet();
  }

  @Override
  public NavigableMap<String, V> descendingMap() {
    return mQualifierMap.descendingMap();
  }

  @Override
  public java.util.Map.Entry<String, V> firstEntry() {
    return mQualifierMap.firstEntry();
  }

  @Override
  public java.util.Map.Entry<String, V> floorEntry(String key) {
    return mQualifierMap.floorEntry(key);
  }

  @Override
  public String floorKey(String key) {
    return mQualifierMap.floorKey(key);
  }

  @Override
  public NavigableMap<String, V> headMap(String toKey, boolean inclusive) {
    return mQualifierMap.headMap(toKey, inclusive);
  }

  @Override
  public SortedMap<String, V> headMap(String toKey) {
    return mQualifierMap.headMap(toKey);
  }

  @Override
  public java.util.Map.Entry<String, V> higherEntry(String key) {
    return mQualifierMap.higherEntry(key);
  }

  @Override
  public String higherKey(String key) {
    return mQualifierMap.higherKey(key);
  }

  @Override
  public java.util.Map.Entry<String, V> lastEntry() {
    return mQualifierMap.lastEntry();
  }

  @Override
  public java.util.Map.Entry<String, V> lowerEntry(String key) {
    return mQualifierMap.lowerEntry(key);
  }

  @Override
  public String lowerKey(String key) {
    return mQualifierMap.lowerKey(key);
  }

  @Override
  public NavigableSet<String> navigableKeySet() {
    return mQualifierMap.navigableKeySet();
  }

  @Override
  public java.util.Map.Entry<String, V> pollFirstEntry() {
    return mQualifierMap.pollFirstEntry();
  }

  @Override
  public java.util.Map.Entry<String, V> pollLastEntry() {
    return mQualifierMap.pollLastEntry();
  }

  @Override
  public NavigableMap<String, V> subMap(String fromKey, boolean fromInclusive,
      String toKey, boolean toInclusive) {
    return mQualifierMap.subMap(fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public SortedMap<String, V> subMap(String fromKey, String toKey) {
    return mQualifierMap.subMap(fromKey, toKey);
  }

  @Override
  public NavigableMap<String, V> tailMap(String fromKey, boolean inclusive) {
    return mQualifierMap.tailMap(fromKey, inclusive);
  }

  @Override
  public SortedMap<String, V> tailMap(String fromKey) {
    // TODO Auto-generated method stub
    return mQualifierMap.tailMap(fromKey);
  }

}
