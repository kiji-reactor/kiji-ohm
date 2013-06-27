package org.kiji.ohm.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface KijiColumn {
  /**
   * Family of the column to fetch from.
   */
  String family();

  /**
   * Qualifier of the column to fetch from.
   * An empty string means no qualifier, ie. fetch from a map-type family.
   */
  String qualifier() default "";

  /**
   * Maximum number of versions to fetch.
   * 1 means to fetch at most one version (ie. not a time-series).
   */
  int maxVersions() default 1;

  /**
   * When greater than 0, paging is enabled.
   * 0 means no paging.
   */
  int pageSize() default 0;
}
