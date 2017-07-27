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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.concurrent.Execs;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.MetadataTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.indexing.test.TestDataSegmentAnnouncer;
import io.druid.indexing.test.TestDataSegmentKiller;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.metadata.EntryExistsException;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.metadata.SQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Druids;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPusherConfig;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifier;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.coordination.DataSegmentServerAnnouncer;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class JDBCIndexTaskTest
{
  private static final Logger log = new Logger(JDBCIndexTaskTest.class);
  private static final ObjectMapper objectMapper = TestHelper.getJsonMapper();
  private static final long POLL_RETRY_MS = 100;
  private static final DataSchema DATA_SCHEMA = new DataSchema(
      "test_ds",
      objectMapper.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("created_date", "auto", null),
                  new DimensionsSpec(
                      DimensionsSpec.getDefaultSchemas(ImmutableList.<String>of(
                          "id",
                          "audit_key",
                          "type",
                          "author",
                          "comment",
                          "created_date",
                          "payload"
                      )),
                      null,
                      null
                  ),
                  new JSONPathSpec(true, ImmutableList.<JSONPathFieldSpec>of()),
                  ImmutableMap.<String, Boolean>of()
              ),
              Charsets.UTF_8.name()
          ),
          Map.class
      ),
      new AggregatorFactory[]{new CountAggregatorFactory("rows")},
      new UniformGranularitySpec(Granularities.DAY, Granularities.NONE, null),
      objectMapper
  );
  private static final String tableName = "druid_audit";
  private static final String tsName = "TSCOLUMN";
  private static final List<String> columns = Lists.newArrayList(
      "id",
      "audit_key",
      "type",
      "author",
      "comment",
      "created_date",
      "payload"
  );
  private static ServiceEmitter emitter;
  private static ListeningExecutorService taskExec;
  private static int tablePostfix;
  private static String uri = "jdbc:mysql://emn-g03-02:3306/druid";
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derby = new TestDerbyConnector.DerbyConnectorRule();
  private final List<Task> runningTasks = Lists.newArrayList();
  private final boolean buildV9Directly;
  private long handoffConditionTimeout = 0;
  private boolean reportParseExceptions = false;
  private boolean doHandoff = true;
  private TaskToolboxFactory toolboxFactory;
  private IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private TaskStorage taskStorage;
  private TaskLockbox taskLockbox;
  private File directory;
  private String table;
  private ArrayList arr = new ArrayList();
  private TestDerbyConnector derbyConnector;


  public JDBCIndexTaskTest(boolean buildV9Directly)
  {
    this.buildV9Directly = buildV9Directly;
  }

  private static String getTableName()
  {
    return "table" + tablePostfix++;
  }

  @Parameterized.Parameters(name = "buildV9Directly = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{true}, new Object[]{false});
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    emitter = new ServiceEmitter(
        "service",
        "host",
        new LoggingEmitter(
            log,
            LoggingEmitter.Level.ERROR,
            new DefaultObjectMapper()
        )
    );
    emitter.start();
    EmittingLogger.registerEmitter(emitter);

    taskExec = MoreExecutors.listeningDecorator(
        Executors.newCachedThreadPool(
            Execs.makeThreadFactory("JDBC-task-test-%d")
        )
    );
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    taskExec.shutdown();
    taskExec.awaitTermination(9999, TimeUnit.DAYS);


    emitter.close();
  }

  @Before
  public void setupTest() throws Exception
  {
    handoffConditionTimeout = 0;
    reportParseExceptions = false;
    doHandoff = true;
    table = getTableName();
    derbyConnector = derby.getConnector();
    makeToolboxFactory();


  }

  @After
  public void tearDownTest()
  {
    synchronized (runningTasks) {
      for (Task task : runningTasks) {
        task.stopGracefully();
      }

      runningTasks.clear();
    }

    destroyToolboxFactory();
  }

  @Test(timeout = 60_000L)
  public void testRunAfterDataInserted() throws Exception
  {
    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 10",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(10, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2016-11-25/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2016-08-03/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2017-05-19/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());


    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("5", "6", "7"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("1", "2", "3", "4"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("8", "9", "10"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testRunOnNothing() throws Exception
  {
    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 0",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffOccurs() throws Exception
  {
    handoffConditionTimeout = 5_000;

    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            null,
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(10, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2016-11-25/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2016-08-03/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2017-05-19/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());


    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("5", "6", "7"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("1", "2", "3", "4"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("8", "9", "10"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testHandoffConditionTimeoutWhenHandoffDoesNotOccur() throws Exception
  {
    doHandoff = false;
    handoffConditionTimeout = 100;

    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 1",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.FAILED, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(1, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2016-08-03/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1), publishedDescriptors());


    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("1"), readSegmentDim1(desc1));
  }

  @Test(timeout = 60_000L)
  public void testReportParseExceptions() throws Exception
  {
    reportParseExceptions = true;

    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 0",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(0, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    Assert.assertEquals(ImmutableSet.of(), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));
  }

  @Test(timeout = 60_000L)
  public void testRunReplicas() throws Exception
  {
    final JDBCIndexTask task1 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " where id =1  ",
            columns
        ),
        null,
        null
    );
    final JDBCIndexTask task2 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " where id =1 ",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data


    // Wait for tasks to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(1, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2016-08-03/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1), publishedDescriptors());

    Assert.assertEquals(
        new JDBCDataSourceMetadata(table, 0, 10),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("1"), readSegmentDim1(desc1));
  }

  @Test(timeout = 60_000L)
  public void testRunConflicting() throws Exception
  {
    final JDBCIndexTask task1 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            null,
            columns
        ),
        null,
        null
    );
    final JDBCIndexTask task2 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            null,
            columns
        ),
        null,
        null
    );

    // Insert data


    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(10, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(10, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata, should all be from the first task
    SegmentDescriptor desc1 = SD(task1, "2016-08-03/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2016-11-25/P1D", 0);
    SegmentDescriptor desc3 = SD(task1, "2017-05-19/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());

    Assert.assertEquals(
        new JDBCDataSourceMetadata(table, 0, 10),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );
    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("1", "2", "3", "4"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("5", "6", "7"), readSegmentDim1(desc2));
  }

  @Test(timeout = 60_000L)
  public void testRunConflictingWithoutTransactions() throws Exception
  {
    final JDBCIndexTask task1 = createTask( //TODO::: transaction option check
                                            null,
                                            new JDBCIOConfig(
                                                "0",
                                                tableName,
                                                "druid",
                                                "druid",
                                                uri,
                                                "com.mysql.jdbc.Driver",
                                                new JDBCOffsets(tableName, 0, 10),
                                                false,
                                                false,
                                                null,
                                                "select * from " + tableName + " where id <= 3",
                                                columns
                                            ),
                                            null,
                                            null
    );

    final JDBCIndexTask task2 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            false,
            false,
            null,
            "select * from " + tableName + " where id > 3 and id <= 6",
            columns
        ),
        null,
        null
    );

    // Insert data


    // Run first task
    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2016-08-03/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Run second task
    final ListenableFuture<TaskStatus> future2 = runTask(task2);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(3, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(3, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc2 = SD(task2, "2016-11-25/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertNull(metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource()));

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("1", "2", "3"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("5", "6"), readSegmentDim1(desc2));

  }


  @Test(timeout = 60_000L)
  public void testRunTwoTasksTwoPartitions() throws Exception
  {
    final JDBCIndexTask task1 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            null,
            columns
        ),
        null,
        null
    );
    final JDBCIndexTask task2 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            null,
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);
    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert data


    // Wait for tasks to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(10, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(10, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task1, "2016-11-25/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2016-08-03/P1D", 0);
    SegmentDescriptor desc3 = SD(task2, "2017-05-19/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());


    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("5", "6", "7"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("1", "2", "3", "4"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("8", "9", "10"), readSegmentDim1(desc3));
  }

  @Test(timeout = 60_000L)
  public void testRestore() throws Exception
  {
    final JDBCIndexTask task1 = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 1",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future1 = runTask(task1);

    // Insert some data, but not enough for the task to finish


    while (countEvents(task1) != 2) {
      Thread.sleep(25);
    }

    Assert.assertEquals(2, countEvents(task1));

    // Stop without publishing segment
    task1.stopGracefully();
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future1.get().getStatusCode());

    // Start a new task
    final JDBCIndexTask task2 = createTask(
        task1.getId(),
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 1",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future2 = runTask(task2);

    // Insert remaining data


    // Wait for task to exit
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(2, task1.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task1.getFireDepartmentMetrics().thrownAway());
    Assert.assertEquals(1, task2.getFireDepartmentMetrics().processed());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task2.getFireDepartmentMetrics().thrownAway());

    // Check published segments & metadata
    SegmentDescriptor desc1 = SD(task1, "2010/P1D", 0);
    SegmentDescriptor desc2 = SD(task1, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2), publishedDescriptors());
    Assert.assertEquals(
        new JDBCDataSourceMetadata(table, 0, 10),
        metadataStorageCoordinator.getDataSourceMetadata(DATA_SCHEMA.getDataSource())
    );

    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc2));
  }


  @Test(timeout = 60_000L)
  public void testRunAndPauseAfterReadWithModifiedEndOffsets() throws Exception
  {
    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 4",
            columns
        ),
        null,
        null
    );

    final ListenableFuture<TaskStatus> future = runTask(task);

    while (task.getStatus() != JDBCIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    // reached the end of the assigned offsets and paused instead of publishing
    Assert.assertEquals(task.getEndOffsets(), task.getCurrentOffsets());
    Assert.assertEquals(JDBCIndexTask.Status.PAUSED, task.getStatus());

    Assert.assertEquals(ImmutableMap.of(0, 3L), task.getEndOffsets());
    AtomicInteger newEndOffsets = new AtomicInteger();
    task.setEndOffsets(newEndOffsets, false);
    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
    Assert.assertEquals(JDBCIndexTask.Status.PAUSED, task.getStatus());
    task.resume();

    while (task.getStatus() != JDBCIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    // reached the end of the updated offsets and paused
    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
    Assert.assertEquals(JDBCIndexTask.Status.PAUSED, task.getStatus());

    // try again but with resume flag == true

    Assert.assertEquals(newEndOffsets, task.getEndOffsets());
    Assert.assertNotEquals(JDBCIndexTask.Status.PAUSED, task.getStatus());

    while (task.getStatus() != JDBCIndexTask.Status.PAUSED) {
      Thread.sleep(25);
    }

    Assert.assertEquals(newEndOffsets, task.getCurrentOffsets());
    Assert.assertEquals(JDBCIndexTask.Status.PAUSED, task.getStatus());

    task.resume();

    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get().getStatusCode());

    // Check metrics
    Assert.assertEquals(4, task.getFireDepartmentMetrics().processed());
    Assert.assertEquals(1, task.getFireDepartmentMetrics().unparseable());
    Assert.assertEquals(0, task.getFireDepartmentMetrics().thrownAway());

    // Check published metadata
    SegmentDescriptor desc1 = SD(task, "2009/P1D", 0);
    SegmentDescriptor desc2 = SD(task, "2010/P1D", 0);
    SegmentDescriptor desc3 = SD(task, "2011/P1D", 0);
    Assert.assertEquals(ImmutableSet.of(desc1, desc2, desc3), publishedDescriptors());


    // Check segments in deep storage
    Assert.assertEquals(ImmutableList.of("b"), readSegmentDim1(desc1));
    Assert.assertEquals(ImmutableList.of("c"), readSegmentDim1(desc2));
    Assert.assertEquals(ImmutableList.of("d", "e"), readSegmentDim1(desc3));
  }

  @Test(timeout = 30_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndPause() throws Exception
  {
    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 1",
            columns
        ),
        null,
        null
    );

    runTask(task);

    while (!task.getStatus().equals(JDBCIndexTask.Status.READING)) {
      Thread.sleep(2000);
    }

    task.pause(0);

    while (!task.getStatus().equals(JDBCIndexTask.Status.PAUSED)) {
      Thread.sleep(25);
    }
  }

  @Test(timeout = 30_000L)
  public void testRunWithOffsetOutOfRangeExceptionAndNextOffsetGreaterThanLeastAvailable() throws Exception
  {
    // Insert data


    final JDBCIndexTask task = createTask(
        null,
        new JDBCIOConfig(
            "0",
            tableName,
            "druid",
            "druid",
            uri,
            "com.mysql.jdbc.Driver",
            new JDBCOffsets(tableName, 0, 10),
            true,
            false,
            null,
            "select * from " + tableName + " limit 5",
            columns
        ),
        null,
        true
    );

    runTask(task);

    while (!task.getStatus().equals(JDBCIndexTask.Status.READING)) {
      Thread.sleep(20);
    }

    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(task.getStatus(), JDBCIndexTask.Status.READING);
      // Offset should not be reset
//      Assert.assertTrue(task.getCurrentOffsets() == 100L);
    }
  }

  private ListenableFuture<TaskStatus> runTask(final Task task)
  {
    try {
      taskStorage.insert(task, TaskStatus.running(task.getId()));
    }
    catch (EntryExistsException e) {
      // suppress
    }
    taskLockbox.syncFromStorage();
    final TaskToolbox toolbox = toolboxFactory.build(task);
    synchronized (runningTasks) {
      runningTasks.add(task);
    }
    return taskExec.submit(
        new Callable<TaskStatus>()
        {
          @Override
          public TaskStatus call() throws Exception
          {
            try {
              if (task.isReady(toolbox.getTaskActionClient())) {
                return task.run(toolbox);
              } else {
                throw new ISE("Task is not ready");
              }
            }
            catch (Exception e) {
              log.warn(e, "Task failed");
              return TaskStatus.failure(task.getId());
            }
          }
        }
    );
  }

  private TaskLock getLock(final Task task, final Interval interval)
  {
    return Iterables.find(
        taskLockbox.findLocksForTask(task),
        new Predicate<TaskLock>()
        {
          @Override
          public boolean apply(TaskLock lock)
          {
            return lock.getInterval().contains(interval);
          }
        }
        , null
    );
  }

  private JDBCIndexTask createTask(
      final String taskId,
      final JDBCIOConfig ioConfig,
      final Integer maxRowsPerSegment,
      final Boolean resetOffsetAutomatically
  )
  {
    final JDBCTuningConfig tuningConfig = new JDBCTuningConfig(
        1000,
        maxRowsPerSegment,
        new Period("P1Y"),
        null,
        null,
        null,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically
    );
    final JDBCIndexTask task = new JDBCIndexTask(
        taskId,
        null,
        DATA_SCHEMA,
        tuningConfig,
        ioConfig,
        null,
        null
    );
    task.setPollRetryMs(POLL_RETRY_MS);
    return task;
  }

  private QueryRunnerFactoryConglomerate makeTimeseriesOnlyConglomerate()
  {
    return new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    new IntervalChunkingQueryRunnerDecorator(null, null, null)
                    {
                      @Override
                      public <T> QueryRunner<T> decorate(
                          QueryRunner<T> delegate, QueryToolChest<T, ? extends Query<T>> toolChest
                      )
                      {
                        return delegate;
                      }
                    }
                ),
                new TimeseriesQueryEngine(),
                new QueryWatcher()
                {
                  @Override
                  public void registerQuery(Query query, ListenableFuture future)
                  {
                    // do nothing
                  }
                }
            )
        )
    );
  }

  private void makeToolboxFactory() throws IOException
  {
    directory = tempFolder.newFolder();
    final TestUtils testUtils = new TestUtils();
    final ObjectMapper objectMapper = testUtils.getTestObjectMapper();
    for (Module module : new JDBCIndexTaskModule().getJacksonModules()) {
      objectMapper.registerModule(module);
    }
    final TaskConfig taskConfig = new TaskConfig(
        new File(directory, "taskBaseDir").getPath(),
        null,
        null,
        50000,
        null,
        false,
        null,
        null
    );

    derbyConnector.createDataSourceTable();
    derbyConnector.createPendingSegmentsTable();
    derbyConnector.createSegmentTable();
    derbyConnector.createRulesTable();
    derbyConnector.createConfigTable();
    derbyConnector.createTaskTables();
    derbyConnector.createAuditTable();
    taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new SQLMetadataStorageActionHandlerFactory(
            derbyConnector,
            derby.metadataTablesConfigSupplier().get(),
            objectMapper
        )
    );
    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        testUtils.getTestObjectMapper(),
        derby.metadataTablesConfigSupplier().get(),
        derbyConnector
    );
    taskLockbox = new TaskLockbox(taskStorage);
    final TaskActionToolbox taskActionToolbox = new TaskActionToolbox(
        taskLockbox,
        metadataStorageCoordinator,
        emitter,
        new SupervisorManager(null)
    );
    final TaskActionClientFactory taskActionClientFactory = new LocalTaskActionClientFactory(
        taskStorage,
        taskActionToolbox
    );
    final SegmentHandoffNotifierFactory handoffNotifierFactory = new SegmentHandoffNotifierFactory()
    {
      @Override
      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource)
      {
        return new SegmentHandoffNotifier()
        {
          @Override
          public boolean registerSegmentHandoffCallback(
              SegmentDescriptor descriptor, Executor exec, Runnable handOffRunnable
          )
          {
            if (doHandoff) {
              // Simulate immediate handoff
              exec.execute(handOffRunnable);
            }
            return true;
          }

          @Override
          public void start()
          {
            //Noop
          }

          @Override
          public void close()
          {
            //Noop
          }
        };
      }
    };
    final LocalDataSegmentPusherConfig dataSegmentPusherConfig = new LocalDataSegmentPusherConfig();
    dataSegmentPusherConfig.storageDirectory = getSegmentDirectory();
    final DataSegmentPusher dataSegmentPusher = new LocalDataSegmentPusher(dataSegmentPusherConfig, objectMapper);
    toolboxFactory = new TaskToolboxFactory(
        taskConfig,
        taskActionClientFactory,
        emitter,
        dataSegmentPusher,
        new TestDataSegmentKiller(),
        null, // DataSegmentMover
        null, // DataSegmentArchiver
        new TestDataSegmentAnnouncer(),
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        makeTimeseriesOnlyConglomerate(),
        MoreExecutors.sameThreadExecutor(), // queryExecutorService
        EasyMock.createMock(MonitorScheduler.class),
        new SegmentLoaderFactory(
            new SegmentLoaderLocalCacheManager(
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }, testUtils.getTestObjectMapper()
            )
        ),
        testUtils.getTestObjectMapper(),
        testUtils.getTestIndexMerger(),
        testUtils.getTestIndexIO(),
        MapCache.create(1024),
        new CacheConfig(),
        testUtils.getTestIndexMergerV9()
    );
  }

  private void destroyToolboxFactory()
  {
    toolboxFactory = null;
    taskStorage = null;
    taskLockbox = null;
    metadataStorageCoordinator = null;
  }

  private Set<SegmentDescriptor> publishedDescriptors() throws IOException
  {
    return FluentIterable.from(
        metadataStorageCoordinator.getUsedSegmentsForInterval(
            DATA_SCHEMA.getDataSource(),
            new Interval("0000/3000")
        )
    ).transform(
        new Function<DataSegment, SegmentDescriptor>()
        {
          @Override
          public SegmentDescriptor apply(DataSegment input)
          {
            return input.toDescriptor();
          }
        }
    ).toSet();
  }

  private File getSegmentDirectory()
  {
    return new File(directory, "segments");
  }

  private List<String> readSegmentDim1(final SegmentDescriptor descriptor) throws IOException
  {
    File indexZip = new File(
        String.format(
            "%s/%s/%s_%s/%s/%d/index.zip",
            getSegmentDirectory(),
            DATA_SCHEMA.getDataSource(),
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    File outputLocation = new File(
        directory,
        String.format(
            "%s_%s_%s_%s",
            descriptor.getInterval().getStart(),
            descriptor.getInterval().getEnd(),
            descriptor.getVersion(),
            descriptor.getPartitionNumber()
        )
    );
    outputLocation.mkdir();
    CompressionUtils.unzip(
        Files.asByteSource(indexZip),
        outputLocation,
        Predicates.<Throwable>alwaysFalse(),
        false
    );
    IndexIO indexIO = new TestUtils().getTestIndexIO();
    QueryableIndex index = indexIO.loadIndex(outputLocation);
    DictionaryEncodedColumn<String> dim1 = index.getColumn("id").getDictionaryEncoding();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < dim1.length(); i++) {
      int id = dim1.getSingleValueRow(i);
      String value = dim1.lookupName(id);
      values.add(value);
    }
    return values;
  }

  public long countEvents(final Task task) throws Exception
  {
    // Do a query.
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(DATA_SCHEMA.getDataSource())
                                  .aggregators(
                                      ImmutableList.<AggregatorFactory>of(
                                          new LongSumAggregatorFactory("rows", "rows")
                                      )
                                  ).granularity(Granularities.ALL)
                                  .intervals("0000/3000")
                                  .build();

    ArrayList<Result<TimeseriesResultValue>> results = Sequences.toList(
        task.getQueryRunner(query).run(query, ImmutableMap.<String, Object>of()),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    return results.isEmpty() ? 0 : results.get(0).getValue().getLongMetric("rows");
  }


  private SegmentDescriptor SD(final Task task, final String intervalString, final int partitionNum)
  {
    final Interval interval = new Interval(intervalString);
    return new SegmentDescriptor(interval, getLock(task, interval).getVersion(), partitionNum);

  }

}
