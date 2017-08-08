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

package io.druid.indexing.jdbc.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.druid.indexing.jdbc.JDBCOffsets;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.List;

public class JDBCSupervisorIOConfig {

  private final String table;
  private final String user;
  private final String password;
  private final String connectURI;
  private final String driverClass;
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Duration startDelay;
  private final Duration period;
  private final Duration completionTimeout;
  private final Optional<Duration> lateMessageRejectionPeriod;
  private final JDBCOffsets jdbcOffsets;
  private final String query;
  private final List<String> columns;
  private final int interval;

  @JsonCreator
  public JDBCSupervisorIOConfig(
      @JsonProperty("table") String table,
      @JsonProperty("user") String user,
      @JsonProperty("password") String password,
      @JsonProperty("connectURI") String connectURI,
      @JsonProperty("driverClass") String driverClass,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("jdbcOffsets") JDBCOffsets jdbcOffsets,
      @JsonProperty("query") String query,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("interval") int interval
  ) {
    this.table = Preconditions.checkNotNull(table, "table");
    this.user = Preconditions.checkNotNull(user, "user");
    this.password = Preconditions.checkNotNull(password, "password");
    this.connectURI = Preconditions.checkNotNull(connectURI, "jdbc:derby:memory:myDB;create=true");
    this.driverClass = Preconditions.checkNotNull(driverClass, "org.apache.derby.jdbc.EmbeddedDriver");
    this.replicas = replicas != null ? replicas : 1;
    this.taskCount = taskCount != null ? taskCount : 1;
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.completionTimeout = defaultDuration(completionTimeout, "PT30M");
    this.lateMessageRejectionPeriod = lateMessageRejectionPeriod == null
        ? Optional.<Duration>absent()
        : Optional.of(lateMessageRejectionPeriod.toStandardDuration());
    this.jdbcOffsets = jdbcOffsets;
    this.query = query;
    this.columns = columns;
    this.interval = interval;
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
  public String getTable() {
    return table;
  }

  @JsonProperty
  public Integer getReplicas() {
    return replicas;
  }

  @JsonProperty
  public Integer getTaskCount() {
    return taskCount;
  }

  @JsonProperty
  public Duration getTaskDuration() {
    return taskDuration;
  }

  @JsonProperty
  public Duration getStartDelay() {
    return startDelay;
  }

  @JsonProperty
  public Duration getPeriod() {
    return period;
  }

  @JsonProperty
  public Duration getCompletionTimeout() {
    return completionTimeout;
  }

  @JsonProperty
  public Optional<Duration> getLateMessageRejectionPeriod() {
    return lateMessageRejectionPeriod;
  }

  @JsonProperty
  public int getInterval() {
    return interval;
  }

  @JsonProperty
  public JDBCOffsets getJdbcOffsets() {
    return jdbcOffsets;
  }

  @Override
  public String toString() {
    return "JDBCSupervisorIOConfig{" +
        "table='" + table + '\'' +
        ", user='" + user + '\'' +
        ", password='" + password + '\'' +
        ", connectURI='" + connectURI + '\'' +
        ", driverClass='" + driverClass + '\'' +
        ", replicas=" + replicas +
        ", taskCount=" + taskCount +
        ", taskDuration=" + taskDuration +
        ", startDelay=" + startDelay +
        ", period=" + period +
        ", completionTimeout=" + completionTimeout +
        ", lateMessageRejectionPeriod=" + lateMessageRejectionPeriod +
        ", jdbcOffsets=" + jdbcOffsets +
        ", query='" + query + '\'' +
        ", columns=" + columns +
        ", interval=" + interval +
        '}';
  }

  private static Duration defaultDuration(final Period period, final String theDefault) {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }
}
