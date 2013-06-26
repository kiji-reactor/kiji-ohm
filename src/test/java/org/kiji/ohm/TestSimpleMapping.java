package org.kiji.ohm;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

public class TestSimpleMapping extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestSimpleMapping.class);

  private static final String USER_TABLE_LAYOUT = "org/kiji/ohm/user_table.json";

  private KijiTable mTable;

  @Before
  public final void setup() throws Exception {
    final Kiji kiji = new InstanceBuilder(getKiji())
        .withTable(KijiTableLayouts.getLayout(USER_TABLE_LAYOUT))
        .build();
    mTable = kiji.openTable("user_table");
  }

  @After
  public final void teardown() throws Exception {
    mTable.release();
    mTable = null;
  }

  @Test
  public void testSimpleMapping() throws Exception {
  }
}
