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

import java.util.Map;

public class JDBCOffsets
{
  private final String table;
  private final Map<Integer, Long> offsetMaps;

  @JsonCreator
  public JDBCOffsets(
      @JsonProperty("table") final String table,
      @JsonProperty("offsetMaps") final Map<Integer, Long> offsetMaps
  )
  {
    this.table = table;
    this.offsetMaps = offsetMaps;

  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }

  @JsonProperty
  public Map<Integer, Long> getOffsetMaps()
  {
    return offsetMaps;
  }


  @Override
  public String toString()
  {
    return "JDBCOffsets{" +
           "table='" + table + '\'' +
           ", offsetMaps=" + offsetMaps +
           '}';
  }
}
