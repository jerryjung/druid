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

public class JDBCIOConfig implements IOConfig {
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_PAUSE_AFTER_READ = false;

  private final String baseSequenceName;
  private final String tableName;
  private final String user;
  private final String password;
  private final String connectURI;
  private final String driverClass;
  private final JDBCOffsets jdbcOffsets;
  private final boolean useTransaction;
  private final boolean pauseAfterRead;
  private final Optional<DateTime> minimumMessageTime;
  private final String query;
  private final List<String> columns;
  private final int interval;

  @JsonCreator
  public JDBCIOConfig(
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("user") String user,
      @JsonProperty("password") String password,
      @JsonProperty("connectURI") String connectURI,
      @JsonProperty("driverClass") String driverClass,
      @JsonProperty("jdbcOffsets") JDBCOffsets jdbcOffsets,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("pauseAfterRead") Boolean pauseAfterRead,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("query") String query,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("interval") int interval
  ) {
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.tableName = Preconditions.checkNotNull(tableName, "tableName");
    this.user = Preconditions.checkNotNull(user, "user");
    this.password = Preconditions.checkNotNull(password, "password");
    this.connectURI = Preconditions.checkNotNull(connectURI, "jdbc:derby:memory:myDB;create=true");
    this.driverClass = Preconditions.checkNotNull(driverClass, "org.apache.derby.jdbc.EmbeddedDriver");
    this.jdbcOffsets = jdbcOffsets;
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.pauseAfterRead = pauseAfterRead != null ? pauseAfterRead : DEFAULT_PAUSE_AFTER_READ;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
    this.query = query;
    this.columns = columns;
    this.interval = interval;

  }

  @JsonProperty
  public JDBCOffsets getJdbcOffsets() {
    return jdbcOffsets;
  }

  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  @JsonProperty
  public String getUser() {
    return user;
  }

  @JsonProperty
  public String getPassword() {
    return password;
  }

  @JsonProperty
  public String getConnectURI() {
    return connectURI;
  }

  @JsonProperty
  public String getDriverClass() {
    return driverClass;
  }

  @JsonProperty
  public String getBaseSequenceName() {
    return baseSequenceName;
  }

  @JsonProperty
  public boolean isUseTransaction() {
    return useTransaction;
  }

  @JsonProperty
  public boolean isPauseAfterRead() {
    return pauseAfterRead;
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime() {
    return minimumMessageTime;
  }

  @JsonProperty
  public String getQuery() {
    return query;
  }

  @JsonProperty
  public List<String> getColumns() {
    return columns;
  }

  @JsonProperty
  public int getInterval() {
    return interval;
  }

  @Override
  public String toString() {
    return "JDBCIOConfig{" +
        "baseSequenceName='" + baseSequenceName + '\'' +
        ", tableName='" + tableName + '\'' +
        ", user='" + user + '\'' +
        ", password='" + password + '\'' +
        ", connectURI='" + connectURI + '\'' +
        ", driverClass='" + driverClass + '\'' +
        ", jdbcOffsets=" + jdbcOffsets +
        ", useTransaction=" + useTransaction +
        ", pauseAfterRead=" + pauseAfterRead +
        ", minimumMessageTime=" + minimumMessageTime +
        ", query='" + query + '\'' +
        ", columns=" + columns +
        ", interval=" + interval +
        '}';
  }
}
