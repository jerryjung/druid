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

public class JDBCPartitions
{
  private final String table;
  private final Integer offset;

  @JsonCreator
  public JDBCPartitions(
      @JsonProperty("table") final String table,
      @JsonProperty("offset") final Integer offset
  )
  {
    this.table = table;
    this.offset = offset;

  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }

  @JsonProperty
  public Integer getOffset()
  {
    return offset;
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

    JDBCPartitions that = (JDBCPartitions) o;

    if (table != null ? !table.equals(that.table) : that.table != null) {
      return false;
    }
    return offset != null ? offset.equals(that.offset) : that.offset == null;

  }

  @Override
  public int hashCode()
  {
    int result = table != null ? table.hashCode() : 0;
    result = 31 * result + (offset != null ? offset.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "JDBCPartitions{" +
           "table='" + table + '\'' +
           ", offset=" + offset +
           '}';
  }
}
