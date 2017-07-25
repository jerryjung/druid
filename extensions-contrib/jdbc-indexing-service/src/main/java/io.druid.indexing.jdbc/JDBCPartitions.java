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
  private final Integer startOffset;
  private final Integer endOffset;

  @JsonCreator
  public JDBCPartitions(
      @JsonProperty("table") final String table,
      @JsonProperty("startOffset") final Integer startOffset,
      @JsonProperty("endOffset") final Integer endOffset
  )
  {
    this.table = table;
    this.startOffset = startOffset;
    this.endOffset = endOffset;

  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }


  @JsonProperty
  public Integer getStartOffset()
  {
    return startOffset;
  }

  @JsonProperty
  public Integer getEndOffset()
  {
    return endOffset;
  }

  @Override
  public String toString()
  {
    return "JDBCPartitions{" +
           "table='" + table + '\'' +
           ", startOffset=" + startOffset +
           ", endOffset=" + endOffset +
           '}';
  }
}
