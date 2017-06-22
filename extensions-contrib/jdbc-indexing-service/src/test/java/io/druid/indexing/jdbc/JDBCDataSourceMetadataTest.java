/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.jdbc;

import org.junit.Assert;
import org.junit.Test;

public class JDBCDataSourceMetadataTest
{
  private static final JDBCDataSourceMetadata JM0 = JM("foo", 0);
  private static final JDBCDataSourceMetadata JM1 = JM("foo", 1);
  private static final JDBCDataSourceMetadata JM2 = JM("foo", 2);
  private static final JDBCDataSourceMetadata JM3 = JM("foo", 3);

  @Test
  public void testMatches()
  {
    Assert.assertTrue(JM0.matches(JM0));
    Assert.assertTrue(JM0.matches(JM1));
    Assert.assertTrue(JM0.matches(JM2));
    Assert.assertTrue(JM0.matches(JM3));

    Assert.assertTrue(JM1.matches(JM0));
    Assert.assertTrue(JM1.matches(JM1));
    Assert.assertTrue(JM1.matches(JM2));
    Assert.assertTrue(JM1.matches(JM3));

    Assert.assertTrue(JM2.matches(JM0));
    Assert.assertTrue(JM2.matches(JM1));
    Assert.assertTrue(JM2.matches(JM2));
    Assert.assertTrue(JM2.matches(JM3));

    Assert.assertTrue(JM3.matches(JM0));
    Assert.assertTrue(JM3.matches(JM1));
    Assert.assertTrue(JM3.matches(JM2));
    Assert.assertTrue(JM3.matches(JM3));
  }

  @Test
  public void testIsValidStart()
  {
    Assert.assertTrue(JM0.isValidStart());
    Assert.assertTrue(JM1.isValidStart());
    Assert.assertTrue(JM2.isValidStart());
    Assert.assertTrue(JM3.isValidStart());
  }

  @Test
  public void testPlus()
  {
    Assert.assertEquals(
        JM("foo", 1),
        JM1.plus(JM3)
    );

    Assert.assertEquals(
        JM("foo", 0),
        JM0.plus(JM2)
    );

    Assert.assertEquals(
        JM("foo", 2),
        JM1.plus(JM2)
    );

    Assert.assertEquals(
        JM("foo", 3),
        JM2.plus(JM1)
    );

    Assert.assertEquals(
        JM("foo", 4),
        JM2.plus(JM2)
    );
  }

  @Test
  public void testMinus()
  {
    Assert.assertEquals(
        JM("foo", 1),
        JM1.minus(JM3)
    );

    Assert.assertEquals(
        JM("foo", 2),
        JM0.minus(JM2)
    );

    Assert.assertEquals(
        JM("foo", 3),
        JM1.minus(JM2)
    );

    Assert.assertEquals(
        JM("foo", 2),
        JM2.minus(JM1)
    );

    Assert.assertEquals(
        JM("foo", 1),
        JM2.minus(JM2)
    );
  }

  private static JDBCDataSourceMetadata JM(String datasource, int offsets)
  {
    return new JDBCDataSourceMetadata(datasource, offsets);
  }
}
