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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.jdbc.JDBCDataSourceMetadata;
import io.druid.indexing.jdbc.JDBCIOConfig;
import io.druid.indexing.jdbc.JDBCIndexTask;
import io.druid.indexing.jdbc.JDBCIndexTaskClient;
import io.druid.indexing.jdbc.JDBCIndexTaskClientFactory;
import io.druid.indexing.jdbc.JDBCOffsets;
import io.druid.indexing.jdbc.JDBCTuningConfig;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerListener;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.TestHelper;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.server.metrics.DruidMonitorSchedulerConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import org.apache.curator.test.TestingCluster;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

@RunWith(Parameterized.class)
public class JDBCSupervisorTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = TestHelper.getJsonMapper();
  private static final String TABLE_PREFIX = "druid_audit";
  private static final String DATASOURCE = "testDS";
  private static final int TEST_CHAT_THREADS = 3;
  private static final long TEST_CHAT_RETRIES = 9L;
  private static final Period TEST_HTTP_TIMEOUT = new Period("PT10S");
  private static final Period TEST_SHUTDOWN_TIMEOUT = new Period("PT80S");
  private static final Map<Integer, Integer> offsets = new HashMap();
  private static final int interval = 10;

  private static TestingCluster zkServer;
  private static DataSchema dataSchema;

  private final int numThreads;

  private JDBCSupervisor supervisor;
  private JDBCSupervisorTuningConfig tuningConfig;
  private TaskStorage taskStorage;
  private TaskMaster taskMaster;
  private TaskRunner taskRunner;
  private IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private JDBCIndexTaskClient taskClient;
  private TaskQueue taskQueue;
  private String table;

  private static String getTable()
  {
    return TABLE_PREFIX;
  }

  @Parameterized.Parameters(name = "numThreads = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{1}, new Object[]{8});
  }

  public JDBCSupervisorTest(int numThreads)
  {
    this.numThreads = numThreads;
  }

  @BeforeClass
  public static void setupClass() throws Exception
  {
    zkServer = new TestingCluster(1);
    zkServer.start();
    dataSchema = getDataSchema(DATASOURCE);
  }

  @Before
  public void setupTest() throws Exception
  {
    taskStorage = createMock(TaskStorage.class);
    taskMaster = createMock(TaskMaster.class);
    taskRunner = createMock(TaskRunner.class);
    indexerMetadataStorageCoordinator = createMock(IndexerMetadataStorageCoordinator.class);
    taskClient = createMock(JDBCIndexTaskClient.class);
    taskQueue = createMock(TaskQueue.class);

    tuningConfig = new JDBCSupervisorTuningConfig(
        1000,
        50000,
        new Period("P1Y"),
        new File("/test"),
        null,
        null,
        true,
        false,
        null,
        null,
        numThreads,
        TEST_CHAT_THREADS,
        TEST_CHAT_RETRIES,
        TEST_HTTP_TIMEOUT,
        TEST_SHUTDOWN_TIMEOUT,
        null
    );

    table = getTable();
  }

  @After
  public void tearDownTest() throws Exception
  {
    supervisor = null;
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    zkServer.stop();
    zkServer = null;
  }

  @Test
  public void testNoInitialState() throws Exception
  {

    supervisor = getSupervisor(2, 1, "PT1H", null);

    DateTime now = DateTime.now();
    Task id1 = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        now
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    // check that replica tasks are created with the same minimumMessageTime as tasks inherited from another supervisor
    Assert.assertEquals(now, ((JDBCIndexTask) captured.getValue()).getIOConfig().getMinimumMessageTime().get());

    // test that a task failing causes a new task to be re-queued with the same parameters
    String runningTaskId = captured.getValue().getId();
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    JDBCIndexTask iHaveFailed = (JDBCIndexTask) existingTasks.get(0);
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getStatus(runningTaskId)).andReturn(Optional.of(TaskStatus.running(runningTaskId))).anyTimes();
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync(runningTaskId)).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((JDBCIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );

    // check that failed tasks are recreated with the same minimumMessageTime as the task it replaced, even if that
    // task came from another supervisor
    Assert.assertEquals(now, ((JDBCIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMinimumMessageTime().get());

  }

  @Test
  public void testNewTaskFromMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task = captured.getValue();
    Assert.assertEquals(dataSchema, task.getDataSchema());
    Assert.assertEquals(JDBCTuningConfig.copyOf(tuningConfig), task.getTuningConfig());

    JDBCIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", taskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", taskConfig.isPauseAfterRead());
    Assert.assertFalse("minimumMessageTime", taskConfig.getMinimumMessageTime().isPresent());


    Assert.assertEquals(table, taskConfig.getPartitions().getTable());
    Assert.assertEquals(taskConfig.getPartitions().getOffsetMaps().values().toArray()[0], 10);

  }


  @Test
  public void testSkipOffsetGaps() throws Exception
  {
    supervisor = getSupervisor(1, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task = captured.getValue();
    JDBCIOConfig taskConfig = task.getIOConfig();

  }


  @Test
  public void testReplicas() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task1 = captured.getValues().get(0);
    Assert.assertEquals(task1.getIOConfig().getPartitions().getOffsetMaps().keySet().toArray()[0], 0);

    JDBCIndexTask task2 = captured.getValues().get(1);
    Assert.assertEquals(task2.getIOConfig().getPartitions().getOffsetMaps().values().toArray()[0], 10);
  }

  @Test
  public void testLateMessageRejectionPeriod() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task1 = captured.getValues().get(0);
    JDBCIndexTask task2 = captured.getValues().get(1);

//    Assert.assertTrue(
//        "minimumMessageTime",
//        task1.getIOConfig().getMinimumMessageTime().get().plusMinutes(59).isBeforeNow()
//    );
//    Assert.assertTrue(
//        "minimumMessageTime",
//        task1.getIOConfig().getMinimumMessageTime().get().plusMinutes(61).isAfterNow()
//    );
//    Assert.assertEquals(
//        task1.getIOConfig().getMinimumMessageTime().get(),
//        task2.getIOConfig().getMinimumMessageTime().get()
//    );
  }

  @Test
  /**
   * Test generating the starting offsets from the partition high water marks in JDBC.
   */
  public void testLatestOffset() throws Exception
  {
    supervisor = getSupervisor(1, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            null
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task = captured.getValue();
    Assert.assertEquals(task.getIOConfig().getPartitions().getOffsetMaps().values().toArray()[0], 10);
  }

  @Test
  /**
   * Test generating the starting offsets from the partition data stored in druid_dataSource which contains the
   * offsets of the last built segments.
   */
  public void testDatasourceMetadata() throws Exception
  {
    supervisor = getSupervisor(1, 1, "PT1H", null);

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.<TaskRunner>absent()).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(new JDBCOffsets(table, offsets, interval))
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    JDBCIndexTask task = captured.getValue();
    JDBCIOConfig taskConfig = task.getIOConfig();
    Assert.assertEquals("sequenceName-0", taskConfig.getBaseSequenceName());
    Assert.assertEquals(task.getIOConfig().getPartitions().getOffsetMaps().values().toArray()[0], 10);
  }

  @Test
  public void testKillIncompatibleTasks() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);
    Task id1 = createJDBCIndexTask( // unexpected # of partitions (kill)
                                    "id1",
                                    DATASOURCE,
                                    "index_jdbc_testDS__some_other_sequenceName",
                                    new JDBCOffsets(table, offsets, interval),
                                    null
    );

    Task id2 = createJDBCIndexTask( // correct number of partitions and ranges (don't kill)
                                    "id2",
                                    DATASOURCE,
                                    "sequenceName-0",
                                    new JDBCOffsets(table, offsets, interval),
                                    null
    );

    Task id3 = createJDBCIndexTask( // unexpected range on partition 2 (kill)
                                    "id3",
                                    DATASOURCE,
                                    "index_jdbc_testDS__some_other_sequenceName",
                                    new JDBCOffsets(table, offsets, interval),
                                    null
    );

    Task id4 = createJDBCIndexTask( // different datasource (don't kill)
                                    "id4",
                                    "other-datasource",
                                    "index_jdbc_testDS_d927edff33c4b3f",
                                    new JDBCOffsets(table, offsets, interval),
                                    null
    );

    Task id5 = new RealtimeIndexTask( // non JDBCIndexTask (don't kill)
                                      "id5",
                                      null,
                                      new FireDepartment(
                                          dataSchema,
                                          new RealtimeIOConfig(null, null, null),
                                          null
                                      ),
                                      null
    );

    List<Task> existingTasks = ImmutableList.of(id1, id2, id3, id4, id5);

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.stopAsync("id1", false)).andReturn(Futures.immediateFuture(true));
    expect(taskClient.stopAsync("id3", false)).andReturn(Futures.immediateFuture(false));
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    taskQueue.shutdown("id3");

    expect(taskQueue.add(anyObject(Task.class))).andReturn(true);
    replayAll();
    supervisor.start();
    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testRequeueTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);
    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    // test that running the main loop again checks the status of the tasks that were created and does nothing if they
    // are all still running
    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task failing causes a new task to be re-queued with the same parameters
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    List<Task> imStillAlive = tasks.subList(0, 2);
    JDBCIndexTask iHaveFailed = (JDBCIndexTask) tasks.get(1);
    reset(taskStorage);
    reset(taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(imStillAlive).anyTimes();
    for (Task task : imStillAlive) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
//    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
//    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
//    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);

    supervisor.runInternal();
    verifyAll();

//    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
//    Assert.assertEquals(
//        iHaveFailed.getIOConfig().getBaseSequenceName(),
//        ((JDBCIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
//    );
  }

  @Test
  public void testRequeueAdoptedTaskWhenFailed() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);

    DateTime now = DateTime.now();
    Task id1 = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        now
    );

    List<Task> existingTasks = ImmutableList.of(id1);

    Capture<Task> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(existingTasks).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id1")).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true);
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    // check that replica tasks are created with the same minimumMessageTime as tasks inherited from another supervisor
    Assert.assertEquals(now, ((JDBCIndexTask) captured.getValue()).getIOConfig().getMinimumMessageTime().get());

    // test that a task failing causes a new task to be re-queued with the same parameters
    String runningTaskId = captured.getValue().getId();
    Capture<Task> aNewTaskCapture = Capture.newInstance();
    JDBCIndexTask iHaveFailed = (JDBCIndexTask) existingTasks.get(0);
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(captured.getValue())).anyTimes();
    expect(taskStorage.getStatus(iHaveFailed.getId())).andReturn(Optional.of(TaskStatus.failure(iHaveFailed.getId())));
    expect(taskStorage.getStatus(runningTaskId)).andReturn(Optional.of(TaskStatus.running(runningTaskId))).anyTimes();
    expect(taskStorage.getTask(iHaveFailed.getId())).andReturn(Optional.of((Task) iHaveFailed)).anyTimes();
    expect(taskStorage.getTask(runningTaskId)).andReturn(Optional.of(captured.getValue())).anyTimes();
    expect(taskClient.getStatusAsync(runningTaskId)).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync(runningTaskId)).andReturn(Futures.immediateFuture(now)).anyTimes();
    expect(taskQueue.add(capture(aNewTaskCapture))).andReturn(true);
    replay(taskStorage);
    replay(taskQueue);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    Assert.assertNotEquals(iHaveFailed.getId(), aNewTaskCapture.getValue().getId());
    Assert.assertEquals(
        iHaveFailed.getIOConfig().getBaseSequenceName(),
        ((JDBCIndexTask) aNewTaskCapture.getValue()).getIOConfig().getBaseSequenceName()
    );

    // check that failed tasks are recreated with the same minimumMessageTime as the task it replaced, even if that
    // task came from another supervisor
    Assert.assertEquals(now, ((JDBCIndexTask) aNewTaskCapture.getValue()).getIOConfig().getMinimumMessageTime().get());
  }

  @Test
  public void testQueueNextTasksOnSuccess() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(taskClient.getStatusAsync(anyString())).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.NOT_STARTED))
                                                  .anyTimes();
    expect(taskClient.getStartTimeAsync(anyString())).andReturn(Futures.immediateFuture(DateTime.now())).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    reset(taskStorage);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    replay(taskStorage);

    supervisor.runInternal();
    verifyAll();

    // test that a task succeeding causes a new task to be re-queued with the next offset range and causes any replica
    // tasks to be shutdown
    Capture<Task> newTasksCapture = Capture.newInstance(CaptureType.ALL);
    Capture<String> shutdownTaskIdCapture = Capture.newInstance();
    List<Task> imStillRunning = tasks.subList(1, 2);
    JDBCIndexTask iAmSuccess = (JDBCIndexTask) tasks.get(0);
    reset(taskStorage);
    reset(taskQueue);
    reset(taskClient);
    expect(taskStorage.getActiveTasks()).andReturn(imStillRunning).anyTimes();
    for (Task task : imStillRunning) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskStorage.getStatus(iAmSuccess.getId())).andReturn(Optional.of(TaskStatus.success(iAmSuccess.getId())));
    expect(taskStorage.getTask(iAmSuccess.getId())).andReturn(Optional.of((Task) iAmSuccess)).anyTimes();
    expect(taskQueue.add(capture(newTasksCapture))).andReturn(true).times(2);
    expect(taskClient.stopAsync(capture(shutdownTaskIdCapture), eq(false))).andReturn(Futures.immediateFuture(true));
    replay(taskStorage);
    replay(taskQueue);
    replay(taskClient);

    supervisor.runInternal();
    verifyAll();

    // make sure we killed the right task (sequenceName for replicas are the same)
    Assert.assertTrue(shutdownTaskIdCapture.getValue().contains(iAmSuccess.getIOConfig().getBaseSequenceName()));
  }

  @Test
  public void testDiscoverExistingPublishingTask() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, "PT1H", null);

    Task task = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(new HashMap<>(0,10));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof JDBCSupervisorReport.JDBCSupervisorReportPayload);

    JDBCSupervisorReport.JDBCSupervisorReportPayload payload = (JDBCSupervisorReport.JDBCSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(table, payload.getTable());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
//    Assert.assertEquals(publishingReport.getOffsets().values().toArray()[0], 10);


    JDBCIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(JDBCTuningConfig.copyOf(tuningConfig), capturedTask.getTuningConfig());

    JDBCIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", capturedTaskConfig.isPauseAfterRead());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(table, capturedTaskConfig.getPartitions().getTable());
    Assert.assertEquals(capturedTaskConfig.getPartitions().getOffsetMaps().values().toArray()[0], 10);

  }

  @Test
  public void testDiscoverExistingPublishingTaskWithDifferentPartitionAllocation() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(1, 1, "PT1H", null);

    Task task = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));

    Capture<JDBCIndexTask> captured = Capture.newInstance();
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(task)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(task)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.PUBLISHING));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(new HashMap<>(0,10));
    expect(taskQueue.add(capture(captured))).andReturn(true);

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof JDBCSupervisorReport.JDBCSupervisorReportPayload);

    JDBCSupervisorReport.JDBCSupervisorReportPayload payload = (JDBCSupervisorReport.JDBCSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(table, payload.getTable());
    Assert.assertEquals(0, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id1", publishingReport.getId());
//    Assert.assertEquals(publishingReport.getOffsets().values().toArray()[0], 10);

    JDBCIndexTask capturedTask = captured.getValue();
    Assert.assertEquals(dataSchema, capturedTask.getDataSchema());
    Assert.assertEquals(JDBCTuningConfig.copyOf(tuningConfig), capturedTask.getTuningConfig());

    JDBCIOConfig capturedTaskConfig = capturedTask.getIOConfig();
    Assert.assertEquals("sequenceName-0", capturedTaskConfig.getBaseSequenceName());
    Assert.assertTrue("isUseTransaction", capturedTaskConfig.isUseTransaction());
    Assert.assertFalse("pauseAfterRead", capturedTaskConfig.isPauseAfterRead());

    // check that the new task was created with starting offsets matching where the publishing task finished
    Assert.assertEquals(table, capturedTaskConfig.getPartitions().getTable());
    Assert.assertEquals(capturedTaskConfig.getPartitions().getOffsetMaps().values().toArray()[0], 10);

  }

  @Test
  public void testDiscoverExistingPublishingAndReadingTask() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(1, 1, "PT1H", null);

    Task id1 = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Task id2 = createJDBCIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsetsAsync("id1", false))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(new HashMap<>(0,10));
    expect(taskClient.getCurrentOffsetsAsync("id2", false))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    supervisor.updateCurrentAndLatestOffsets().run();
    SupervisorReport report = supervisor.getStatus();
    verifyAll();

    Assert.assertEquals(DATASOURCE, report.getId());
    Assert.assertTrue(report.getPayload() instanceof JDBCSupervisorReport.JDBCSupervisorReportPayload);

    JDBCSupervisorReport.JDBCSupervisorReportPayload payload = (JDBCSupervisorReport.JDBCSupervisorReportPayload)
        report.getPayload();

    Assert.assertEquals(DATASOURCE, payload.getDataSource());
    Assert.assertEquals(3600L, (long) payload.getDurationSeconds());
    Assert.assertEquals(1, (int) payload.getReplicas());
    Assert.assertEquals(table, payload.getTable());
    Assert.assertEquals(1, payload.getActiveTasks().size());
    Assert.assertEquals(1, payload.getPublishingTasks().size());

    TaskReportData activeReport = payload.getActiveTasks().get(0);
    TaskReportData publishingReport = payload.getPublishingTasks().get(0);

    Assert.assertEquals("id2", activeReport.getId());
    Assert.assertEquals(startTime, activeReport.getStartTime());
//    Assert.assertEquals(activeReport.getOffsets().values().toArray()[0], 10);
//    Assert.assertEquals(activeReport.getLag().values().toArray()[0], 10);

    Assert.assertEquals("id1", publishingReport.getId());
//    Assert.assertEquals(publishingReport.getOffsets().values().toArray()[0], 10);
//    Assert.assertEquals(null, publishingReport.getLag());

    Assert.assertEquals(payload.getLatestOffsets().values().toArray()[0], 10);
    Assert.assertEquals(payload.getMinimumLag().values().toArray()[0], 10);
    Assert.assertEquals(10L, (long) payload.getAggregateLag());
    Assert.assertTrue(payload.getOffsetsLastUpdated().plusMinutes(1).isAfterNow());
  }

  @Test
  public void testKillUnresponsiveTasksWhileGettingStartTime() throws Exception
  {
    supervisor = getSupervisor(2, 1, "PT1H", null);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();

    reset(taskStorage, taskClient, taskQueue);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
      expect(taskClient.getStatusAsync(task.getId()))
          .andReturn(Futures.immediateFuture(JDBCIndexTask.Status.NOT_STARTED));
      expect(taskClient.getStartTimeAsync(task.getId()))
          .andReturn(Futures.<DateTime>immediateFailedFuture(new RuntimeException()));
      taskQueue.shutdown(task.getId());
    }
    replay(taskStorage, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();
  }

  @Test
  public void testKillUnresponsiveTasksWhilePausing() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 1, "PT1M", null);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();
    Collection workItems = new ArrayList<>();
    for (Task task : tasks) {
      workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));
    }

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTime.now().minusMinutes(1)))
        .andReturn(Futures.immediateFuture(DateTime.now()));
//    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-1")))
//        .andReturn(Futures.immediateFuture(DateTime.now()))
//        .times(1);
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.<Map<Integer, Integer>>immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      JDBCIOConfig taskConfig = ((JDBCIndexTask) task).getIOConfig();
      Assert.assertEquals(taskConfig.getPartitions().getOffsetMaps().values().toArray()[0], 10);
    }
  }

  @Test
  public void testKillUnresponsiveTasksWhileSettingEndOffsets() throws Exception
  {
    final TaskLocation location = new TaskLocation("testHost", 1234, -1);

    supervisor = getSupervisor(2, 1, "PT1M", null);

    Capture<Task> captured = Capture.newInstance(CaptureType.ALL);
    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(Collections.EMPTY_LIST).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.<Task>of()).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);
    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    List<Task> tasks = captured.getValues();
    Collection workItems = new ArrayList<>();
    for (Task task : tasks) {
      workItems.add(new TestTaskRunnerWorkItem(task.getId(), null, location));
    }

    reset(taskStorage, taskRunner, taskClient, taskQueue);
    captured = Capture.newInstance(CaptureType.ALL);
    expect(taskStorage.getActiveTasks()).andReturn(tasks).anyTimes();
    for (Task task : tasks) {
      expect(taskStorage.getStatus(task.getId())).andReturn(Optional.of(TaskStatus.running(task.getId()))).anyTimes();
      expect(taskStorage.getTask(task.getId())).andReturn(Optional.of(task)).anyTimes();
    }
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.getStatusAsync(anyString()))
        .andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING))
        .anyTimes();
    expect(taskClient.getStartTimeAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(DateTime.now().minusMinutes(2)))
        .andReturn(Futures.immediateFuture(DateTime.now()));
    expect(taskClient.pauseAsync(EasyMock.contains("sequenceName-0")))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));
    expect(
        taskClient.setEndOffsetsAsync(
            EasyMock.contains("sequenceName-0"),
            EasyMock.eq(new HashMap(0, 1)),
            EasyMock.eq(true)
        )
    ).andReturn(Futures.<Boolean>immediateFailedFuture(new RuntimeException())).times(2);
    taskQueue.shutdown(EasyMock.contains("sequenceName-0"));
    expectLastCall().times(2);
    expect(taskQueue.add(capture(captured))).andReturn(true).times(2);

    replay(taskStorage, taskRunner, taskClient, taskQueue);

    supervisor.runInternal();
    verifyAll();

    for (Task task : captured.getValues()) {
      JDBCIOConfig taskConfig = ((JDBCIndexTask) task).getIOConfig();
      Assert.assertEquals(taskConfig.getPartitions().getOffsetMaps().values().toArray()[0], 10);
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testStopNotStarted() throws Exception
  {
    supervisor = getSupervisor(1, 1, "PT1H", null);
    supervisor.stop(false);
  }

  @Test
  public void testStop() throws Exception
  {
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    taskClient.close();
    taskRunner.unregisterListener(String.format("JDBCSupervisor-%s", DATASOURCE));
    replayAll();

    supervisor = getSupervisor(1, 1, "PT1H", null);
    supervisor.start();
    supervisor.stop(false);

    verifyAll();
  }

  @Test
  public void testStopGracefully() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(2, 1, "PT1H", null);

    Task id1 = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Task id2 = createJDBCIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Task id3 = createJDBCIndexTask(
        "id3",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(new HashMap<>(0,10));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskRunner, taskClient, taskQueue);
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskClient.pauseAsync("id2"))
        .andReturn(Futures.immediateFuture(new HashMap<>(0,10)));
//    expect(taskClient.setEndOffsetsAsync("id2",new HashMap<>()))  //TODO:::endOffset value check
//        .andReturn(Futures.immediateFuture(true));
    taskQueue.shutdown("id2");
    taskQueue.shutdown("id3");
    expectLastCall().times(2);

    replay(taskRunner, taskClient, taskQueue);

    supervisor.gracefulShutdownInternal();
    verifyAll();
  }


  @Test
  public void testResetRunningTasks() throws Exception
  {
    final TaskLocation location1 = new TaskLocation("testHost", 1234, -1);
    final TaskLocation location2 = new TaskLocation("testHost2", 145, -1);
    final DateTime startTime = new DateTime();

    supervisor = getSupervisor(2, 1, "PT1H", null);

    Task id1 = createJDBCIndexTask(
        "id1",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Task id2 = createJDBCIndexTask(
        "id2",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Task id3 = createJDBCIndexTask(
        "id3",
        DATASOURCE,
        "sequenceName-0",
        new JDBCOffsets(table, offsets, interval),
        null
    );

    Collection workItems = new ArrayList<>();
    workItems.add(new TestTaskRunnerWorkItem(id1.getId(), null, location1));
    workItems.add(new TestTaskRunnerWorkItem(id2.getId(), null, location2));

    expect(taskMaster.getTaskQueue()).andReturn(Optional.of(taskQueue)).anyTimes();
    expect(taskMaster.getTaskRunner()).andReturn(Optional.of(taskRunner)).anyTimes();
    expect(taskRunner.getRunningTasks()).andReturn(workItems).anyTimes();
    expect(taskStorage.getActiveTasks()).andReturn(ImmutableList.of(id1, id2, id3)).anyTimes();
    expect(taskStorage.getStatus("id1")).andReturn(Optional.of(TaskStatus.running("id1"))).anyTimes();
    expect(taskStorage.getStatus("id2")).andReturn(Optional.of(TaskStatus.running("id2"))).anyTimes();
    expect(taskStorage.getStatus("id3")).andReturn(Optional.of(TaskStatus.running("id3"))).anyTimes();
    expect(taskStorage.getTask("id1")).andReturn(Optional.of(id1)).anyTimes();
    expect(taskStorage.getTask("id2")).andReturn(Optional.of(id2)).anyTimes();
    expect(taskStorage.getTask("id3")).andReturn(Optional.of(id3)).anyTimes();
    expect(indexerMetadataStorageCoordinator.getDataSourceMetadata(DATASOURCE)).andReturn(
        new JDBCDataSourceMetadata(
            new JDBCOffsets(table, offsets, interval)
        )
    ).anyTimes();
    expect(taskClient.getStatusAsync("id1")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.PUBLISHING));
    expect(taskClient.getStatusAsync("id2")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStatusAsync("id3")).andReturn(Futures.immediateFuture(JDBCIndexTask.Status.READING));
    expect(taskClient.getStartTimeAsync("id2")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getStartTimeAsync("id3")).andReturn(Futures.immediateFuture(startTime));
    expect(taskClient.getCurrentOffsets("id1", true)).andReturn(new HashMap<>(0,10));

    taskRunner.registerListener(anyObject(TaskRunnerListener.class), anyObject(Executor.class));
    replayAll();

    supervisor.start();
    supervisor.runInternal();
    verifyAll();

    reset(taskQueue, indexerMetadataStorageCoordinator);
    expect(indexerMetadataStorageCoordinator.deleteDataSourceMetadata(DATASOURCE)).andReturn(true);
    taskQueue.shutdown("id2");
    taskQueue.shutdown("id3");
    replay(taskQueue, indexerMetadataStorageCoordinator);

    supervisor.resetInternal(null);
    verifyAll();
  }


  private JDBCSupervisor getSupervisor(
      int replicas,
      int taskCount,
      String duration,
      Period lateMessageRejectionPeriod
  )
  {

    offsets.put(0,10);
    JDBCSupervisorIOConfig jdbcSupervisorIOConfig = new JDBCSupervisorIOConfig(
        table,
        "druid",
        "druid",
        "jdbc:mysql://emn-g03-02:3306/druid",
        "com.mysql.jdbc.Driver",
        replicas,
        taskCount,
        new Period(duration),
        new Period("P1D"),
        new Period("PT30S"),
        new JDBCOffsets(table, offsets, interval),
        new Period("PT30M"),
        lateMessageRejectionPeriod,
        null,
        Lists.newArrayList(
            "id",
            "audit_key",
            "type",
            "author",
            "comment",
            "created_date",
            "payload"
        )
    );

    JDBCIndexTaskClientFactory taskClientFactory = new JDBCIndexTaskClientFactory(null, null)
    {
      @Override
      public JDBCIndexTaskClient build(
          TaskInfoProvider taskInfoProvider,
          String dataSource,
          int numThreads,
          Duration httpTimeout,
          long numRetries
      )
      {
        Assert.assertEquals(TEST_CHAT_THREADS, numThreads);
        Assert.assertEquals(TEST_HTTP_TIMEOUT.toStandardDuration(), httpTimeout);
        Assert.assertEquals(TEST_CHAT_RETRIES, numRetries);
        return taskClient;
      }
    };

    return new TestableJDBCSupervisor(
        taskStorage,
        taskMaster,
        indexerMetadataStorageCoordinator,
        taskClientFactory,
        objectMapper,
        new JDBCSupervisorSpec(
            dataSchema,
            tuningConfig,
            jdbcSupervisorIOConfig,
            null,
            taskStorage,
            taskMaster,
            indexerMetadataStorageCoordinator,
            taskClientFactory,
            objectMapper,
            new NoopServiceEmitter(),
            new DruidMonitorSchedulerConfig()
        )
    );
  }

  private static DataSchema getDataSchema(String dataSource)
  {
    return new DataSchema(
        dataSource,
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
        new UniformGranularitySpec(
            Granularities.HOUR,
            Granularities.NONE,
            ImmutableList.<Interval>of()
        ),
        objectMapper
    );
  }

  private JDBCIndexTask createJDBCIndexTask(
      String id,
      String dataSource,
      String sequenceName,
      JDBCOffsets partition,
      DateTime minimumMessageTime
  )
  {

    offsets.put(0,10);
    return new JDBCIndexTask(
        id,
        null,
        getDataSchema(dataSource),
        tuningConfig,
        new JDBCIOConfig(
            sequenceName,
            table,
            "druid",
            "druid",
            "jdbc:mysql://emn-g03-02:3306/druid",
            "com.mysql.jdbc.Driver",
            partition,
            true,
            false,
            minimumMessageTime,
            null,
            Lists.newArrayList(
                "id",
                "audit_key",
                "type",
                "author",
                "comment",
                "created_date",
                "payload"
            )
        ),
        ImmutableMap.<String, Object>of(),
        null
    );
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {

    private TaskLocation location;

    public TestTaskRunnerWorkItem(String taskId, ListenableFuture<TaskStatus> result, TaskLocation location)
    {
      super(taskId, result);
      this.location = location;
    }

    @Override
    public TaskLocation getLocation()
    {
      return location;
    }
  }

  private static class TestableJDBCSupervisor extends JDBCSupervisor
  {
    public TestableJDBCSupervisor(
        TaskStorage taskStorage,
        TaskMaster taskMaster,
        IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
        JDBCIndexTaskClientFactory taskClientFactory,
        ObjectMapper mapper,
        JDBCSupervisorSpec spec
    )
    {
      super(taskStorage, taskMaster, indexerMetadataStorageCoordinator, taskClientFactory, mapper, spec);
    }

    @Override
    protected String generateSequenceName(int groupId)
    {
      return String.format("sequenceName-%d", groupId);
    }
  }
}
