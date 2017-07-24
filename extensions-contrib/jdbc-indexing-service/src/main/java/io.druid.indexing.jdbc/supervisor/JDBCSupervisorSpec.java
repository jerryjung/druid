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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.indexing.jdbc.JDBCIndexTaskClientFactory;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.server.metrics.DruidMonitorSchedulerConfig;

import java.util.List;
import java.util.Map;

public class JDBCSupervisorSpec implements SupervisorSpec
{
  private final DataSchema dataSchema;
  private final JDBCSupervisorTuningConfig tuningConfig;
  private final JDBCSupervisorIOConfig ioConfig;
  private final Map<String, Object> context;

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final JDBCIndexTaskClientFactory jdbcIndexTaskClientFactory;
  private final ObjectMapper mapper;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;

  @JsonCreator
  public JDBCSupervisorSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") JDBCSupervisorTuningConfig tuningConfig,
      @JsonProperty("ioConfig") JDBCSupervisorIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject TaskStorage taskStorage,
      @JacksonInject TaskMaster taskMaster,
      @JacksonInject IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      @JacksonInject JDBCIndexTaskClientFactory jdbcIndexTaskClientFactory,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject ServiceEmitter emitter,
      @JacksonInject DruidMonitorSchedulerConfig monitorSchedulerConfig
      )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.tuningConfig = tuningConfig != null
                        ? tuningConfig
                        : new JDBCSupervisorTuningConfig(
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null
                        );
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.context = context;

    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.jdbcIndexTaskClientFactory = jdbcIndexTaskClientFactory;
    this.mapper = mapper;
    this.emitter = emitter;
    this.monitorSchedulerConfig = monitorSchedulerConfig;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public JDBCSupervisorTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public JDBCSupervisorIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  @Override
  public String getId()
  {
    return dataSchema.getDataSource();
  }

  public DruidMonitorSchedulerConfig getMonitorSchedulerConfig()
  {
    return monitorSchedulerConfig;
  }

  @Override
  public Supervisor createSupervisor()
  {
    return new JDBCSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        jdbcIndexTaskClientFactory,
        mapper,
        this
    );
  }

  @Override
  public List<String> getDataSources()
  {
    return ImmutableList.of(getDataSchema().getDataSource());
  }

  @Override
  public String toString()
  {
    return "JDBCSupervisorSpec{" +
           "dataSchema=" + dataSchema +
           ", tuningConfig=" + tuningConfig +
           ", ioConfig=" + ioConfig +
           '}';
  }
}
