package org.kiji.ohm.dao;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;

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
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;

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
   * @param klass
   * @param entityId
   * @return
   */
  public <T> T select(Class<T> klass, EntityId entityId) throws IOException {
    return select(
        klass, entityId,
        HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP);
  }

  /**
   * Shortcut for {@link #selectAll(Class, KijiScannerOptions, long, long).
   *
   * @param klass
   * @param options
   * @return
   */
  public <T> Iterator<T> selectAll(Class<T> klass, KijiScannerOptions options) throws IOException {
    return selectAll(
        klass, options,
        HConstants.OLDEST_TIMESTAMP, HConstants.LATEST_TIMESTAMP);
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
      throws IOException {

    final KijiEntity entity = klass.getAnnotation(KijiEntity.class);
    Preconditions.checkArgument(entity != null, "Class '{}' has no @KijiEntity annotation.", klass);

    // TODO: Use a pool of tables and/or table readers
    final KijiTable table = mKiji.openTable(entity.table());
    try {
      // TODO: Support RowKeyFormat?
      final RowKeyFormat2 rowKeyFormat =
          (RowKeyFormat2) table.getLayout().getDesc().getKeysFormat();

      final KijiTableReader reader = table.openTableReader();
      try {
        final KijiDataRequestBuilder builder = KijiDataRequest.builder();
        builder.withTimeRange(startTime, endTime);
        parseColumnRequests(klass, builder, rowKeyFormat);
        final KijiDataRequest dataRequest = builder.build();

        final KijiRowData row = reader.get(entityId, dataRequest);

        try {
          return (T) createEntityFromRow(klass, row, rowKeyFormat);
        } catch (IllegalAccessException iae) {
          throw new RuntimeException(iae);
        }

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
   * @param klass
   * @param options
   * @param startTime
   * @param endTime
   * @return
   */
  public <T> Iterator<T> selectAll(
      Class<T> klass,
      KijiScannerOptions options,
      long startTime,
      long endTime)
      throws IOException {
    throw new NotImplementedException();
  }

  /**
   * Extracts column data requests from an annotated entity class.
   *
   * @param klass
   * @return
   */
  private static void parseColumnRequests(
      Class<?> klass,
      KijiDataRequestBuilder builder,
      RowKeyFormat2 rowKeyFormat) {

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
        LOG.debug("Validating entity ID field '{}' with component name '{}'.",
            field, eidField.component());
        boolean found = false;
        for (final RowKeyComponent rkc : rowKeyFormat.getComponents()) {
          if (rkc.getName().equals(eidField.component())) {
            found = true;
            break;
          }
        }
        Preconditions.checkArgument(found, "Field '{}' maps to unknown entity ID component '{}'.",
            field, eidField.component());

      } else {
        LOG.debug("Ignoring field '{}' with no annotation.", field);
      }
    }
  }

  /**
   * Creates an entity object from a KijiRowData.
   *
   * @param klass Class of the entity object to instantiate and populate.
   * @return a new populated entity object.
   */
  private static <T> T createEntityFromRow(
      Class<T> klass,
      KijiRowData row,
      RowKeyFormat2 rowKeyFormat)
      throws IOException, IllegalAccessException {

    LOG.debug("Creating entity '{}' from row with ID '{}'.", klass, row.getEntityId());
    final T entity = newInstance(klass);
    final EntityId eid = row.getEntityId();

    for (final Field field : klass.getDeclaredFields()) {
      final KijiColumn column = field.getAnnotation(KijiColumn.class);
      final EntityIdField eidField = field.getAnnotation(EntityIdField.class);

      if ((column != null) && (eidField != null)) {
        throw new IllegalArgumentException(String.format(
            "Field '%s' cannot have both @KijiColumn and @EntityIdField annotations.", field));

      } else if (column != null) {
        if (column.qualifier().isEmpty()) {
          LOG.debug("Populating field '{}' from map-type family '{}'.", field, column.family());
          if (column.maxVersions() == 1) {
            // Field is a map: qualifier -> single value:
            throw new NotImplementedException();
          } else {
            // Field is a map: qualifier -> time-series:
            throw new NotImplementedException();
          }

        } else {
          if (column.maxVersions() == 1) {
            // Field represents a single value from a fully-qualified column:
            LOG.debug("Populating field '{}' from column '{}:{}'.",
                field, column.family(), column.qualifier());
            Object value = row.getMostRecentValue(column.family(), column.qualifier());
            if (field.getType() == String.class) {
              // Automatically converts CharSequence to java String if necessary:
              value = value.toString();
            }
            field.set(entity, value);
          } else {
            // Field represents a time-series from a fully-qualified column:
            // TODO: Field is a time-series: implement a TimeSeries class
            throw new NotImplementedException();
          }
        }

      } else if (eidField != null) {
        boolean found = false;
        int index = 0;
        // TODO: Optimize lookup of entity ID components.
        for (final RowKeyComponent rkc : rowKeyFormat.getComponents()) {
          if (rkc.getName().equals(eidField.component())) {
            field.set(entity, eid.getComponentByIndex(index));
            found = true;
            break;
          } else {
            index += 1;
          }
          Preconditions.checkState(found);
        }

      } else {
        LOG.debug("Ignoring field '{}' with no annotation.", field);
      }
    }

    return entity;
  }

  private static <T> T newInstance(Class<T> klass) throws IllegalAccessException {
    try {
      return klass.newInstance();
    } catch (InstantiationException ie) {
      throw new RuntimeException(ie);
    }
  }
}
