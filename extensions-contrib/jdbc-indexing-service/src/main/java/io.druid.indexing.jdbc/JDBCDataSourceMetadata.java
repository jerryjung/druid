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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.java.util.common.IAE;

import java.util.Map;

public class JDBCDataSourceMetadata implements DataSourceMetadata
{
  private JDBCOffsets jdbcOffsets;


  @JsonCreator
  public JDBCDataSourceMetadata(
      @JsonProperty("jdbcOffsets") JDBCOffsets jdbcOffsets
  )
  {
    this.jdbcOffsets = jdbcOffsets;
  }


  @JsonProperty("jdbcOffsets")
  public JDBCOffsets getJdbcOffsets()
  {
    return jdbcOffsets;
  }

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

    if (that.getJdbcOffsets().getTable().equals(jdbcOffsets.getTable())) {
      // Same table, merge offsets.
      final Map<Integer, Long> newMap = Maps.newHashMap();

      for (Map.Entry<Integer, Long> entry : jdbcOffsets.getOffsetMaps().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      for (Map.Entry<Integer, Long> entry : that.jdbcOffsets.getOffsetMaps().entrySet()) {
        newMap.put(entry.getKey(), entry.getValue());
      }

      return new JDBCDataSourceMetadata(new JDBCOffsets(jdbcOffsets.getTable(), newMap));
    } else {
      // Different table, prefer "other".
      return other;
    }
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
    if (that.getJdbcOffsets().getTable().equals(jdbcOffsets.getTable())) {
      // Same table, remove partitions present in "that" from "this"
      final Map<Integer, Long> newMap = Maps.newHashMap();

      for (Map.Entry<Integer, Long> entry : jdbcOffsets.getOffsetMaps().entrySet()) {
        if(!that.getJdbcOffsets().getOffsetMaps().containsKey(entry.getKey())) {
          newMap.put(entry.getKey(), entry.getValue());
        }
      }

      return new JDBCDataSourceMetadata(new JDBCOffsets(jdbcOffsets.getTable(), newMap));
    } else {
      // Different table, prefer "this".
      return this;
    }
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
