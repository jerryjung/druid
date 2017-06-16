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

import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.IAE;

public class JDBCDataSourceMetadata implements DataSourceMetadata
{

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    if (getClass() != other.getClass()) {
      return false;
    }

    return plus(other).equals(other.plus(this));
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    if (!(other instanceof JDBCDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          JDBCDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final JDBCDataSourceMetadata that = (JDBCDataSourceMetadata) other;
    return that;
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    if (!(other instanceof JDBCDataSourceMetadata)) {
      throw new IAE(
          "Expected instance of %s, got %s",
          JDBCDataSourceMetadata.class.getCanonicalName(),
          other.getClass().getCanonicalName()
      );
    }

    final JDBCDataSourceMetadata that = (JDBCDataSourceMetadata) other;
    return this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JDBCDataSourceMetadata that = (JDBCDataSourceMetadata) o;
    return true; //TODO::: check
  }

}
