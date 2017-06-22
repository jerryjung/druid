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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;

import java.util.Map;

public class JDBCIOConfig implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_PAUSE_AFTER_READ = false;
  private static final boolean DEFAULT_SKIP_OFFSET_GAPS = false;

  private final String baseSequenceName;
  private final String tableName;
  private final Integer startPartitions;
  private final boolean useTransaction;
  private final boolean pauseAfterRead;
  private final Optional<DateTime> minimumMessageTime;
  private final boolean skipOffsetGaps;

  @JsonCreator
  public JDBCIOConfig(
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("startPartitions") Integer startPartitions,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("pauseAfterRead") Boolean pauseAfterRead,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps
  )
  {
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    this.startPartitions = startPartitions != null ? startPartitions : 0;
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.pauseAfterRead = pauseAfterRead != null ? pauseAfterRead : DEFAULT_PAUSE_AFTER_READ;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
    this.skipOffsetGaps = skipOffsetGaps != null ? skipOffsetGaps : DEFAULT_SKIP_OFFSET_GAPS;

  }

  @JsonProperty
  public Integer getStartPartitions()
  {
    return startPartitions;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }


  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @JsonProperty
  public boolean isUseTransaction()
  {
    return useTransaction;
  }

  @JsonProperty
  public boolean isPauseAfterRead()
  {
    return pauseAfterRead;
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime()
  {
    return minimumMessageTime;
  }

  @JsonProperty
  public boolean isSkipOffsetGaps()
  {
    return skipOffsetGaps;
  }

  @Override
  public String toString()
  {
    return "JDBCIOConfig{" +
           "baseSequenceName='" + baseSequenceName + '\'' +
           ", tableName=" + tableName +
           ", useTransaction=" + useTransaction +
           ", pauseAfterRead=" + pauseAfterRead +
           ", minimumMessageTime=" + minimumMessageTime +
           ", skipOffsetGaps=" + skipOffsetGaps +
           '}';
  }
}
