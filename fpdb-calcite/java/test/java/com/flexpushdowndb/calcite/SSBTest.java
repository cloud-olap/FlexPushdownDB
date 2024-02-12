package com.flexpushdowndb.calcite;

import org.junit.jupiter.api.Test;

public class SSBTest {

  @Test
  public void testSSB1_1() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/1.1.sql", true);
  }

  @Test
  public void testSSB1_2() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/1.2.sql", true);
  }

  @Test
  public void testSSB1_3() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/1.3.sql", true);
  }

  @Test
  public void testSSB2_1() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/2.1.sql", true);
  }

  @Test
  public void testSSB2_2() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/2.2.sql", true);
  }

  @Test
  public void testSSB2_3() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/2.3.sql", true);
  }

  @Test
  public void testSSB3_1() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/3.1.sql", true);
  }

  @Test
  public void testSSB3_2() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/3.2.sql", true);
  }

  @Test
  public void testSSB3_3() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/3.3.sql", true);
  }

  @Test
  public void testSSB3_4() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/3.4.sql", true);
  }

  @Test
  public void testSSB4_1() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/4.1.sql", true);
  }

  @Test
  public void testSSB4_2() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/4.2.sql", true);
  }

  @Test
  public void testSSB4_3() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/original/4.3.sql", true);
  }

  @Test
  public void testSSB1_1_Typed() throws Exception {
    TestUtil.test("ssb-sf0.01/parquet", "ssb/typed/1.1.sql", true);
  }
}
