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

import java.util.List;

public class JDBCIOConfig implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_PAUSE_AFTER_READ = false;
  private static final boolean DEFAULT_SKIP_OFFSET_GAPS = false;

  private final String baseSequenceName;
  private final String tableName;
  private final String user;
  private final String password;
  private final String connectURI;
  private final String driverClass;
  private final JDBCPartitions startPartitions;
  private final JDBCPartitions endPartitions;
  private final boolean useTransaction;
  private final boolean pauseAfterRead;
  private final Optional<DateTime> minimumMessageTime;
  private final boolean skipOffsetGaps;
  private final String query;
  private final List<String> columns;

  @JsonCreator
  public JDBCIOConfig(
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("user") String user,
      @JsonProperty("password") String password,
      @JsonProperty("connectURI") String connectURI,
      @JsonProperty("driverClass") String driverClass,
      @JsonProperty("startPartitions") JDBCPartitions startPartitions,
      @JsonProperty("endPartitions") JDBCPartitions endPartitions,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("pauseAfterRead") Boolean pauseAfterRead,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps,
      @JsonProperty("query") String query,
      @JsonProperty("columns") List<String> columns
  )
  {
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    this.user = Preconditions.checkNotNull(user, "user");
    this.password = Preconditions.checkNotNull(password, "password");
    this.connectURI = Preconditions.checkNotNull(connectURI, "jdbc:derby:memory:myDB;create=true");
    this.driverClass = Preconditions.checkNotNull(driverClass, "org.apache.derby.jdbc.EmbeddedDriver");
    this.startPartitions = Preconditions.checkNotNull(startPartitions, "startPartitions");
    this.endPartitions = endPartitions;
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.pauseAfterRead = pauseAfterRead != null ? pauseAfterRead : DEFAULT_PAUSE_AFTER_READ;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
    this.skipOffsetGaps = skipOffsetGaps != null ? skipOffsetGaps : DEFAULT_SKIP_OFFSET_GAPS;
    this.query = Preconditions.checkNotNull(query, "select 1");
    this.columns = columns;

    Preconditions.checkArgument(
        startPartitions.getTable().equals(endPartitions.getTable()),
        "start table and end table must match"
    );

    Preconditions.checkArgument(
        endPartitions.getOffset() >= startPartitions.getOffset(),
        "end offset must be >= start offset for partition[%s]"
    );
  }

  @JsonProperty
  public JDBCPartitions getStartPartitions()
  {
    return startPartitions;
  }

  public JDBCPartitions getEndPartitions()
  {
    return endPartitions;
  }

  @JsonProperty
  public String getTableName()
  {
    return tableName;
  }

  @JsonProperty
  public String getUser()
  {
    return user;
  }

  @JsonProperty
  public String getPassword()
  {
    return password;
  }

  @JsonProperty
  public String getConnectURI()
  {
    return connectURI;
  }

  @JsonProperty
  public String getDriverClass()
  {
    return driverClass;
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

  @JsonProperty
  public String getQuery()
  {
    return query;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  public String toString()
  {
    return "JDBCIOConfig{" +
           "baseSequenceName='" + baseSequenceName + '\'' +
           ", tableName='" + tableName + '\'' +
           ", user='" + user + '\'' +
           ", password='" + password + '\'' +
           ", connectURI='" + connectURI + '\'' +
           ", driverClass='" + driverClass + '\'' +
           ", startPartitions=" + startPartitions +
           ", useTransaction=" + useTransaction +
           ", pauseAfterRead=" + pauseAfterRead +
           ", minimumMessageTime=" + minimumMessageTime +
           ", skipOffsetGaps=" + skipOffsetGaps +
           ", query='" + query + '\'' +
           '}';
  }
}
