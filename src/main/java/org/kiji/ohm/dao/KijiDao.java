package org.kiji.ohm.dao;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.ohm.annotations.EntityIdField;
import org.kiji.ohm.annotations.KijiColumn;
import org.kiji.ohm.annotations.KijiEntity;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestBuilder.ColumnsDef;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableReader.KijiScannerOptions;

public class KijiDao implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiDao.class);

  /** Kiji instance. */
  private final Kiji mKiji;

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
    mKiji = instance;

    mKiji.retain();
  }

  @Override
  public void close() throws IOException {
    mKiji.release();
  }

  /**
   * Shortcut for {@link #select(Class, EntityId, long, long).
   *
   * @param targetClass
   * @param eid
   * @return
   */
  public <T> T select(Class<T> targetClass, EntityId eid) throws Exception {
    return select(targetClass,eid,0, Long.MAX_VALUE);
  }

  /**
   * Shortcut for {@link #selectAll(Class, KijiScannerOptions, long, long).
   *
   * @param targetClass
   * @param options
   * @return
   */
  public <T> Iterator<T> selectAll(Class<T> targetClass, KijiScannerOptions options) {
    return selectAll(targetClass, options, 0, Long.MAX_VALUE);
  }

  /**
   * <p> Equivalent of a Kiji get request. </p>
   *
   * @param klass
   * @param entityId
   * @param startTime
   * @param endTime
   * @return
   */
  public <T> T select(Class<T> klass, EntityId entityId, long startTime, long endTime)
      throws Exception {

    final KijiEntity entity = klass.getAnnotation(KijiEntity.class);
    Preconditions.checkArgument(entity != null, "Class '{}' has no @KijiEntity annotation.", klass);

    // TODO: Use a pool of tables and/or table readers
    final KijiTable table = mKiji.openTable(entity.table());
    try {
      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.withTimeRange(startTime, endTime);
        parseColumnRequests(klass, builder);
        final KijiDataRequest dataRequest = builder.build();

        final KijiRowData row = reader.get(entityId, dataRequest);
        return (T) createEntityFromRow(klass, row);

      } finally {
        reader.close();
      }
    } finally {
      table.release();
    }
  }

  /**
   * <p> Equivalent of a Kiji scan. </p>
   *
   * @param targetClass
   * @param options
   * @param startTime
   * @param endTime
   * @return
   */
  public <T> Iterator<T> selectAll(
      Class<T> targetClass,
      KijiScannerOptions options,
      long startTime,
      long endTime) {
    throw new NotImplementedException();
  }

  /**
   * Extracts column data requests from an annotated entity class.
   *
   * @param klass
   * @return
   */
  private static void parseColumnRequests(Class<?> klass, KijiDataRequestBuilder builder) {
    LOG.debug("Parsing column requests from entity '{}'.", klass);

    for (final Field field : klass.getDeclaredFields()) {
      final KijiColumn column = field.getAnnotation(KijiColumn.class);
      final EntityIdField eidField = field.getAnnotation(EntityIdField.class);

      if ((column != null) && (eidField != null)) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' cannot have both @KijiColumn and @EntityIdField annotations.", field));

      } else if (column != null) {
        if (column.qualifier().isEmpty()) {
          // TODO: Must be a map-type family!
          // TODO: Field type must be a map of qualifier -> values/timeseries.

          LOG.debug("Requesting family '{}' for field '{}'.", column.family(), field);
          builder.addColumns(ColumnsDef.create()
              .withMaxVersions(column.maxVersions())
              .addFamily(column.family()));
        } else {
          LOG.debug("Requesting column '{}:{}' for field '{}'.",
              column.family(), column.qualifier(), field);
          builder.addColumns(ColumnsDef.create()
              .withMaxVersions(column.maxVersions())
              .add(column.family(), column.qualifier()));
        }

      } else if (eidField != null) {
        LOG.debug("Ignoring entity ID field '{}'.", field);

      } else {
        LOG.debug("Ignoring field '{}' with no annotation.", field);
      }
    }
  }

  /**
   * Extracts a KijiDataRequest from an annotated entity class.
   *
   * @param klass
   * @return
   */
  private static <T> T createEntityFromRow(Class<T> klass, KijiRowData row)
      throws
          IllegalAccessException,
          InstantiationException,
          IOException {

    LOG.debug("Creating entity '{}' from row with ID '{}'.", klass, row.getEntityId());
    final T entity = klass.newInstance();
    final EntityId eid = row.getEntityId();

    for (final Field field : klass.getDeclaredFields()) {
      final KijiColumn column = field.getAnnotation(KijiColumn.class);
      final EntityIdField eidField = field.getAnnotation(EntityIdField.class);

      if ((column != null) && (eidField != null)) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' cannot have both @KijiColumn and @EntityIdField annotations.", field));

      } else if (column != null) {
        if (column.qualifier().isEmpty()) {
          LOG.debug("Populating field '{}'.", field);
          throw new NotImplementedException();

        } else {
          if (column.maxVersions() == 1) {
            LOG.debug("Populating field '{}' from column '{}:{}'.",
                field, column.family(), column.qualifier());
            field.set(entity, row.getMostRecentValue(column.family(), column.qualifier()));
          } else {
            // TODO: Field is a time-series: implement a TimeSeries class
            throw new NotImplementedException();
          }
        }

      } else if (eidField != null) {
        // TODO: Field is an entity ID
        throw new NotImplementedException();

      } else {
        LOG.debug("Ignoring field '{}' with no annotation.", field);
      }
    }

    return entity;
  }
}
