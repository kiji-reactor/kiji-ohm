package org.kiji.ohm;

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
  }

  // -----------------------------------------------------------------------------------------------

  @KijiEntity(table="user_table")
  public static class User {
    @EntityIdField(component="login")
    public String eidLogin;

    @KijiColumn(family="info", qualifier="login")
    public String login;

    @KijiColumn(family="info", qualifier="full_name")
    public String fullName;

    /** User birth date, in milliseconds since Epoch. */
    @KijiColumn(family="info", qualifier="birth_date")
    public Long birthDate;

    /** User zip code. */
    @KijiColumn(family="info", qualifier="zip_code", maxVersions=HConstants.ALL_VERSIONS)
    public int zipCode;
  }
}
