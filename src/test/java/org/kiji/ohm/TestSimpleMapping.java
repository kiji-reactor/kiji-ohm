package org.kiji.ohm;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Iterator;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.ohm.annotations.EntityIdField;
import org.kiji.ohm.annotations.KijiColumn;
import org.kiji.ohm.annotations.KijiEntity;
import org.kiji.ohm.dao.KijiDao;
import org.kiji.ohm.dao.MapTypeValue;
import org.kiji.ohm.dao.TCell;
import org.kiji.ohm.dao.TimeSeries;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestSimpleMapping extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSimpleMapping.class);

  private static final String USER_TABLE_LAYOUT = "org/kiji/ohm/user_table.json";

  /** Test Kiji instance. Not owned: do not release! */
  private Kiji mKiji;

  /** Test Kiji table. Owned. */
  private KijiTable mTable;

  // -----------------------------------------------------------------------------------------------

  @Before
  public final void setup() throws Exception {
    mKiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(USER_TABLE_LAYOUT))
            .withRow("taton")
                .withFamily("info")
                    .withQualifier("login").withValue("taton")
                    .withQualifier("full_name").withValue("Christophe Taton")
                    .withQualifier("birth_date").withValue(1372272810769L)
                    .withQualifier("zip_code")
                        .withValue(1L, 94110)
                        .withValue(2L, 94131)
            .withRow("missing_cells")
                .withFamily("info")
                    .withQualifier("login").withValue("missing_cells")
             .withRow("amit")
             .withFamily("info")
               .withQualifier("login").withValue("amit")
               .withQualifier("full_name").withValue("Amit N")
             .withFamily("query_count")
               .withQualifier("hello").withValue(1L,20)
               .withQualifier("hello").withValue(2L,30)
               .withQualifier("world").withValue(1L,20)
               .withQualifier("world").withValue(1L,20)
        .build();
    mTable = mKiji.openTable("user_table");
  }

  @After
  public final void teardown() throws Exception {
    mTable.release();
    mTable = null;
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  public void testSimpleMapping() throws Exception {
    final KijiDao dao = new KijiDao(mKiji);
    try {
      final User user = dao.select(User.class, mTable.getEntityId("taton"));
      LOG.debug("Decoded user: {}", user);
      assertEquals("Christophe Taton", user.fullName);
      assertEquals(94131, user.zipCode);
    } finally {
      dao.close();
    }
  }

  @Test
  public void testMissingCells() throws Exception {
    final KijiDao dao = new KijiDao(mKiji);
    try {
      final User user = dao.select(User.class, mTable.getEntityId("missing_cells"));
      LOG.debug("Decoded user: {}", user);
      assertEquals(null, user.fullName);
      assertEquals(0, user.zipCode);
    } finally {
      dao.close();
    }
  }

  @Test
  public void testMultipleGroupVersions() throws Exception {
    final KijiDao dao = new KijiDao(mKiji);
    try {
      final UserMultiVersion user = dao.select(UserMultiVersion.class, mTable.getEntityId("taton"));
      LOG.debug("Decoded user: {}", user);
      assertEquals("Christophe Taton", user.fullName);
      assertEquals(
          Lists.newArrayList(94131, 94110),
          Lists.newArrayList(user.zipCodes.values()));
    } finally {
      dao.close();
    }
  }

  @Test
  public void testMultipleMapVersions() throws Exception {
    final KijiDao dao = new KijiDao(mKiji);
    try {
      final UserMultiVersion user = dao.select(UserMultiVersion.class, mTable.getEntityId("amit"));
      assertEquals("Amit N", user.fullName);
      assertEquals(30, (int) user.queryCount.get("hello").firstEntry().getValue());
    } finally {
      dao.close();
    }
  }
  // -----------------------------------------------------------------------------------------------

  @KijiEntity(table="user_table")
  public static class User {
    @EntityIdField(component="login")
    public String eidLogin;

    @KijiColumn(family="info", qualifier="login")
    public String login;

    @KijiColumn(family="info", qualifier="full_name")
    private String fullName;

    /** User birth date, in milliseconds since Epoch. */
    @KijiColumn(family="info", qualifier="birth_date")
    public Long birthDate;

    /** User zip code. */
    @KijiColumn(family="info", qualifier="zip_code", maxVersions=1)
    public int zipCode;

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("eidLogin", eidLogin)
          .add("login", login)
          .add("fullName", fullName)
          .add("birthDate", birthDate)
          .toString();
    }
  }

  @KijiEntity(table="user_table")
  public static class UserMultiVersion {
    @EntityIdField(component="login")
    public String eidLogin;

    @KijiColumn(family="info", qualifier="login")
    public String login;

    @KijiColumn(family="info", qualifier="full_name")
    private String fullName;

    /** User birth date, in milliseconds since Epoch. */
    @KijiColumn(family="info", qualifier="birth_date")
    public Long birthDate;

    /** User zip code. */
    @KijiColumn(family="info", qualifier="zip_code", maxVersions=HConstants.ALL_VERSIONS)
    public TimeSeries<Integer> zipCodes;

    @KijiColumn(family="query_count", maxVersions=1)
    public MapTypeValue<Integer> queryCount;
  }
}
