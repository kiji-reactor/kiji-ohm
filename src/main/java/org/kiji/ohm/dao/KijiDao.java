package org.kiji.ohm.dao;

import java.lang.annotation.Annotation;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

import org.kiji.ohm.annotations.EntityIdField;
import org.kiji.ohm.annotations.KijiColumn;
import org.kiji.ohm.annotations.KijiEntity;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;

public class KijiDao {

  private Kiji mKijiInstance = null;

  private static Map<Class<? extends Annotation>, String> mAnnotationValidations;

  static {
    mAnnotationValidations = Maps.newHashMap();
    mAnnotationValidations.put(KijiEntity.class, "%s is not annotated as a KijiEntity.");
    mAnnotationValidations.put(EntityIdField.class, "%s does not contain any " +
    		"EntityId annotated fields.");
    mAnnotationValidations.put(KijiColumn.class, "%s does not contain any " +
    		"KijiColumn annotated fields.");
  }

  public KijiDao(Kiji instance) {
    mKijiInstance = instance;
  }
  public <T> T select(Class<T> targetClass, EntityId eid) {
    validateEntity(targetClass);
    //Build KDR here and convert back?
    return select(targetClass,eid,0, Long.MAX_VALUE);
  }

  public <T> Iterator<T> selectAll(Class<T> targetClass, KijiScannerOptions options) {
    validateEntity(targetClass);
  //Build KDR here and convert back?
    return selectAll(targetClass, options, 0, Long.MAX_VALUE);
  }

  public <T> T select(Class<T> targetClass, EntityId eid, long startTime, long endTime) {
    validateEntity(targetClass);
  //Build KDR here and convert back?
    return null;
  }

  public <T> Iterator<T> selectAll(Class<T> targetClass, KijiScannerOptions options,
      long startTime, long endTime) {
    return null;
  }

  private void validateEntity(Class<?> targetClass) throws IllegalArgumentException {
    String simpleName = targetClass.getSimpleName();
    for(Entry<Class<? extends Annotation>, String> e:mAnnotationValidations.entrySet()) {
      if(!targetClass.isAnnotationPresent(e.getKey())) {
        throw new IllegalArgumentException(String.format(e.getValue(), simpleName));
      }
    }
  }
}
