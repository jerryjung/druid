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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.jdbc.*;
import io.druid.indexing.overlord.*;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.EntryExistsException;
import io.druid.server.metrics.DruidMonitorSchedulerConfig;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Supervisor responsible for managing the JDBCIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link JDBCSupervisorSpec} which includes the JDBC table and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the JDBC table
 * and the list of running indexing tasks and ensures that all tuple are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * JDBC offsets.
 */
public class JDBCSupervisor implements Supervisor {
  private static final EmittingLogger log = new EmittingLogger(JDBCSupervisor.class);
  private static final Random RANDOM = new Random();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000; // prevent us from running too often in response to events
  private static final long NOT_SET = 0;
  private static final long MINIMUM_FUTURE_TIMEOUT_IN_SECONDS = 120;
  private static final long MINIMUM_GET_OFFSET_PERIOD_MILLIS = 5000;
  private static final long INITIAL_GET_OFFSET_DELAY_MILLIS = 15000;
  private static final long INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS = 25000;

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by JDBCSupervisor to organize and monitor JDBC offset and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same table and
   * starting from the same offset) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
   * exception being if the supervisor started up and discovered and adopted some already running tasks). At any given
   * time, there should only be up to a maximum of [taskCount] actively-reading task groups (tracked in the [taskGroups]
   * map) + zero or more pending-completion task groups (tracked in [pendingCompletionTaskGroups]).
   */
  private static class TaskGroup {
    final ImmutableMap<Integer, Long> offsetsMap;
    final ConcurrentHashMap<String, TaskData> tasks = new ConcurrentHashMap<>();
    final Optional<DateTime> minimumMessageTime;
    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action

    public TaskGroup(ImmutableMap<Integer, Long> offsets, Optional<DateTime> minimumMessageTime) {
      this.offsetsMap = offsets;
      this.minimumMessageTime = minimumMessageTime;
    }

    Set<String> taskIds() {
      return tasks.keySet();
    }
  }

  private static class TaskData {
    volatile TaskStatus status;
    volatile DateTime startTime;
    volatile Map<Integer, Long> currentOffsets;
  }

  // Map<{group ID}, {actively reading task group}>; see documentation for TaskGroup class
  private final ConcurrentHashMap<Integer, TaskGroup> taskGroups = new ConcurrentHashMap<>();

  // After telling a taskGroup to stop reading and begin publishing a segment, it is moved from [taskGroups] to here so
  // we can monitor its status while we queue new tasks to read the next range of offsets. This is a list since we could
  // have multiple sets of tasks publishing at once if time-to-publish > taskDuration.
  // Map<{group ID}, List<{pending completion task groups}>>
  private final ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>> pendingCompletionTaskGroups = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<Integer, Map<Integer, Long>> groups = new ConcurrentHashMap<>();
  // --------------------------------------------------------

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final JDBCIndexTaskClient taskClient;
  private final ObjectMapper sortingMapper;
  private final JDBCSupervisorSpec spec;
  private final ServiceEmitter emitter;
  private final DruidMonitorSchedulerConfig monitorSchedulerConfig;
  private final String dataSource;
  private final JDBCSupervisorIOConfig ioConfig;
  private final JDBCSupervisorTuningConfig tuningConfig;
  private final JDBCTuningConfig taskTuningConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;
  private final long futureTimeoutInSeconds; // how long to wait for async operations to complete

  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final ScheduledExecutorService reportingExec;
  private final ListeningExecutorService workerExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();
  private final Object stateChangeLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile boolean started = false;
  private volatile boolean stopped = false;
  private volatile Map<Integer, Long> latestOffsetsFromJDBC;
  private volatile DateTime offsetsLastUpdated;

  public JDBCSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final JDBCIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final JDBCSupervisorSpec spec
  ) {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;
    this.emitter = spec.getEmitter();
    this.monitorSchedulerConfig = spec.getMonitorSchedulerConfig();

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.tuningConfig = spec.getTuningConfig();
    this.taskTuningConfig = JDBCTuningConfig.copyOf(this.tuningConfig);
    this.supervisorId = String.format("JDBCSupervisor-%s", dataSource);
    this.exec = Execs.singleThreaded(supervisorId);
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");
    this.reportingExec = Execs.scheduledSingleThreaded(supervisorId + "-Reporting-%d");

    int workerThreads = Math.min(10, this.ioConfig.getTaskCount());
    this.workerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(workerThreads, supervisorId + "-Worker-%d"));
    log.info("Created worker pool with [%d] threads for dataSource [%s]", workerThreads, this.dataSource);

    this.taskInfoProvider = new TaskInfoProvider() {
      @Override
      public TaskLocation getTaskLocation(final String id) {
        Preconditions.checkNotNull(id, "id");
        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
              taskRunner.get().getRunningTasks(), new Predicate<TaskRunnerWorkItem>() {
                @Override
                public boolean apply(TaskRunnerWorkItem taskRunnerWorkItem) {
                  return id.equals(taskRunnerWorkItem.getTaskId());
                }
              }
          );

          if (item.isPresent()) {
            return item.get().getLocation();
          }
        } else {
          log.error("Failed to get task runner because I'm not the leader!");
        }

        return TaskLocation.unknown();
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id) {
        return taskStorage.getStatus(id);
      }
    };

    this.futureTimeoutInSeconds = Math.max(
        MINIMUM_FUTURE_TIMEOUT_IN_SECONDS,
        tuningConfig.getChatRetries() * (tuningConfig.getHttpTimeout().getStandardSeconds()
            + JDBCIndexTaskClient.MAX_RETRY_WAIT_SECONDS)
    );

    int chatThreads = (this.tuningConfig.getChatThreads() != null
        ? this.tuningConfig.getChatThreads()
        : Math.min(10, this.ioConfig.getTaskCount() * this.ioConfig.getReplicas()));
    this.taskClient = taskClientFactory.build(
        taskInfoProvider,
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
    log.info(
        "Created taskClient with dataSource[%s] chatThreads[%d] httpTimeout[%s] chatRetries[%d]",
        dataSource,
        chatThreads,
        this.tuningConfig.getHttpTimeout(),
        this.tuningConfig.getChatRetries()
    );
  }

  @Override
  public void start() {
    synchronized (stateChangeLock) {
      Preconditions.checkState(!started, "already started");
      Preconditions.checkState(!exec.isShutdown(), "already stopped");
      log.info("Supervisor started...");

      try {

        exec.submit(
            new Runnable() {
              @Override
              public void run() {
                try {
                  while (!Thread.currentThread().isInterrupted()) {
                    final Notice notice = notices.take();

                    try {
                      notice.handle();
                    } catch (Throwable e) {
                      log.makeAlert(e, "JDBCSupervisor[%s] failed to handle notice", dataSource)
                          .addData("noticeClass", notice.getClass().getSimpleName())
                          .emit();
                    }
                  }
                } catch (InterruptedException e) {
                  log.info("JDBCSupervisor[%s] interrupted, exiting", dataSource);
                }
              }
            }
        );
        firstRunTime = DateTime.now().plus(ioConfig.getStartDelay());
        log.info("Supervisor started buildRunTask scheduled.............[%d],[%d],[%s]",
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS);
        scheduledExec.scheduleAtFixedRate(
            buildRunTask(),
            ioConfig.getStartDelay().getMillis(),
            Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
            TimeUnit.MILLISECONDS
        );

        reportingExec.scheduleAtFixedRate(
            updateCurrentAndLatestOffsets(),
            ioConfig.getStartDelay().getMillis() + INITIAL_GET_OFFSET_DELAY_MILLIS, // wait for tasks to start up
            Math.max(
                tuningConfig.getOffsetFetchPeriod().getMillis(), MINIMUM_GET_OFFSET_PERIOD_MILLIS
            ),
            TimeUnit.MILLISECONDS
        );

        reportingExec.scheduleAtFixedRate(
            emitLag(),
            ioConfig.getStartDelay().getMillis() + INITIAL_EMIT_LAG_METRIC_DELAY_MILLIS, // wait for tasks to start up
            monitorSchedulerConfig.getEmitterPeriod().getMillis(),
            TimeUnit.MILLISECONDS
        );

        started = true;
        log.info(
            "Started JDBCSupervisor[%s], first run in [%s], with spec: [%s]",
            dataSource,
            ioConfig.getStartDelay(),
            spec.toString()
        );
      } catch (Exception e) {
        log.makeAlert(e, "Exception starting JDBCSupervisor[%s]", dataSource)
            .emit();
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public void stop(boolean stopGracefully) {
    synchronized (stateChangeLock) {
      Preconditions.checkState(started, "not started");

      log.info("Beginning shutdown of JDBCSupervisor[%s]", dataSource);

      try {
        scheduledExec.shutdownNow(); // stop recurring executions
        reportingExec.shutdownNow();

        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          taskRunner.get().unregisterListener(supervisorId);
        }

        // Stopping gracefully will synchronize the end offsets of the tasks and signal them to publish, and will block
        // until the tasks have acknowledged or timed out. We want this behavior when we're explicitly shut down through
        // the API, but if we shut down for other reasons (e.g. we lose leadership) we want to just stop and leave the
        // tasks as they are.
        synchronized (stopLock) {
          if (stopGracefully) {
            log.info("Posting GracefulShutdownNotice, signalling managed tasks to complete and publish");
            notices.add(new GracefulShutdownNotice());
          } else {
            log.info("Posting ShutdownNotice");
            notices.add(new ShutdownNotice());
          }

          long shutdownTimeoutMillis = tuningConfig.getShutdownTimeout().getMillis();
          long endTime = System.currentTimeMillis() + shutdownTimeoutMillis;
          while (!stopped) {
            long sleepTime = endTime - System.currentTimeMillis();
            if (sleepTime <= 0) {
              log.info("Timed out while waiting for shutdown (timeout [%,dms])", shutdownTimeoutMillis);
              stopped = true;
              break;
            }
            stopLock.wait(sleepTime);
          }
        }
        log.info("Shutdown notice handled");

        taskClient.close();
        workerExec.shutdownNow();
        exec.shutdownNow();
        started = false;

        log.info("JDBCSupervisor[%s] has stopped", dataSource);
      } catch (Exception e) {
        log.makeAlert(e, "Exception stopping JDBCSupervisor[%s]", dataSource)
            .emit();
      }
    }
  }

  @Override
  public SupervisorReport getStatus() {
    return generateReport(true);
  }

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata) {
    log.info("Posting ResetNotice");
    notices.add(new ResetNotice(dataSourceMetadata));
  }

  public void possiblyRegisterListener() {
    // getTaskRunner() sometimes fails if the task queue is still being initialized so retry later until we succeed

    if (listenerRegistered) {
      return;
    }

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(
          new TaskRunnerListener() {
            @Override
            public String getListenerId() {
              return supervisorId;
            }

            @Override
            public void locationChanged(final String taskId, final TaskLocation newLocation) {
              // do nothing
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status) {
              log.info("Notice status is " + status.toString());
              notices.add(new RunNotice());
            }
          }, MoreExecutors.sameThreadExecutor()
      );

      listenerRegistered = true;
    }
  }

  private interface Notice {
    void handle() throws ExecutionException, InterruptedException, TimeoutException;
  }

  private class RunNotice implements Notice {
    @Override
    public void handle() throws ExecutionException, InterruptedException, TimeoutException {
      long nowTime = System.currentTimeMillis();
      if (nowTime - lastRunTime < MAX_RUN_FREQUENCY_MILLIS) {
        return;
      }
      lastRunTime = nowTime;

      try {
        log.info("RunInternal called...");
        runInternal();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private class GracefulShutdownNotice extends ShutdownNotice {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException {
      gracefulShutdownInternal();
      super.handle();
    }
  }

  private class ShutdownNotice implements Notice {
    @Override
    public void handle() throws InterruptedException, ExecutionException, TimeoutException {
      synchronized (stopLock) {
        stopped = true;
        stopLock.notifyAll();
      }
    }
  }

  private class ResetNotice implements Notice {
    final DataSourceMetadata dataSourceMetadata;

    ResetNotice(DataSourceMetadata dataSourceMetadata) {
      this.dataSourceMetadata = dataSourceMetadata;
    }

    @Override
    public void handle() {
      log.makeAlert("Resetting dataSource [%s]", dataSource).emit();
      resetInternal(dataSourceMetadata);
    }
  }

  @VisibleForTesting
  void resetInternal(DataSourceMetadata dataSourceMetadata) {
    if (dataSourceMetadata == null) {
      // Reset everything
      boolean result = indexerMetadataStorageCoordinator.deleteDataSourceMetadata(dataSource);
      log.info("Reset dataSource[%s] - dataSource metadata entry deleted? [%s]", dataSource, result);
      killTaskGroupForPartitions(taskGroups.keySet());
    } else if (!(dataSourceMetadata instanceof JDBCDataSourceMetadata)) {
      throw new IAE("Expected JDBCDataSourceMetadata but found instance of [%s]", dataSourceMetadata.getClass());
    } else {
      // Reset only the partitions in dataSourceMetadata if it has not been reset yet
      final JDBCDataSourceMetadata resetJDBCMetadata = (JDBCDataSourceMetadata) dataSourceMetadata;

      if (resetJDBCMetadata.getJdbcOffsets().getTable().equals(ioConfig.getTable())) {
        // metadata can be null
        final DataSourceMetadata metadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
        if (metadata != null && !(metadata instanceof JDBCDataSourceMetadata)) {
          throw new IAE(
              "Expected JDBCDataSourceMetadata from metadata store but found instance of [%s]",
              metadata.getClass()
          );
        }
        final JDBCDataSourceMetadata currentMetadata = (JDBCDataSourceMetadata) metadata;
        boolean metadataUpdateSuccess = false;
        if (currentMetadata == null) {
          metadataUpdateSuccess = true;
        } else {
          final DataSourceMetadata newMetadata = currentMetadata.minus(resetJDBCMetadata);
          try {
            metadataUpdateSuccess = indexerMetadataStorageCoordinator.resetDataSourceMetadata(dataSource, newMetadata);
          } catch (IOException e) {
            log.error("Resetting DataSourceMetadata failed [%s]", e.getMessage());
            Throwables.propagate(e);
          }
        }

      } else {
        log.warn(
            "Reset metadata table [%s] and supervisor's table [%s] do not match",
            resetJDBCMetadata.getJdbcOffsets().getTable(),
            ioConfig.getTable()
        );
      }
    }
  }


  private void killTaskGroupForPartitions(Set<Integer> partitions) {
    for (Integer partition : partitions) {
      TaskGroup taskGroup = taskGroups.get(getTaskGroup(partition));
      if (taskGroup != null) {
        // kill all tasks in this task group
        for (String taskId : taskGroup.tasks.keySet()) {
          log.info("Reset dataSource[%s] - killing task [%s]", dataSource, taskId);
          killTask(taskId);
        }
      }
      groups.remove(getTaskGroup(partition));
      taskGroups.remove(getTaskGroup(partition));
    }
  }

  @VisibleForTesting
  void gracefulShutdownInternal() throws ExecutionException, InterruptedException, TimeoutException {
    // Prepare for shutdown by 1) killing all tasks that haven't been assigned to a worker yet, and 2) causing all
    // running tasks to begin publishing by setting their startTime to a very long time ago so that the logic in
    // checkTaskDuration() will be triggered. This is better than just telling these tasks to publish whatever they
    // have, as replicas that are supposed to publish the same segment may not have read the same set of offsets.
    for (TaskGroup taskGroup : taskGroups.values()) {
      for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
        if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
          killTask(entry.getKey());
        } else {
          entry.getValue().startTime = new DateTime(0);
        }
      }
    }

    checkTaskDuration();
  }

  @VisibleForTesting
  void runInternal() throws ExecutionException, InterruptedException, TimeoutException, IOException {
    possiblyRegisterListener();
    updateDataFromJDBC();
    discoverTasks();
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    checkCurrentTaskState();
    createNewTasks();

    if (log.isDebugEnabled()) {
      log.debug(generateReport(true).toString());
    } else {
      log.info(generateReport(false).toString());
    }
  }

  @VisibleForTesting
  String generateSequenceName(int groupId) {
    StringBuilder sb = new StringBuilder();
    Map<Integer, Long> offsetMaps = taskGroups.get(groupId).offsetsMap;
    for (Map.Entry<Integer, Long> entry : offsetMaps.entrySet()) {
      sb.append(StringUtils.format("+%d(%d)", entry.getKey(), entry.getValue()));
    }

    String offsetStr = sb.toString().substring(1);

    Optional<DateTime> minimumMessageTime = taskGroups.get(groupId).minimumMessageTime;
    String minMsgTimeStr = (minimumMessageTime.isPresent() ? String.valueOf(minimumMessageTime.get().getMillis()) : "");

    String dataSchema, tuningConfig;
    try {
      dataSchema = sortingMapper.writeValueAsString(spec.getDataSchema());
      tuningConfig = sortingMapper.writeValueAsString(taskTuningConfig);
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }

    String hashCode = DigestUtils.sha1Hex(dataSchema + tuningConfig + offsetStr + minMsgTimeStr)
        .substring(0, 15);

    return Joiner.on("_").join("index_jdbc", dataSource, hashCode);
  }

  private static String getRandomId() {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }


  private void updateDataFromJDBC() {

    Map<Integer, Long> partitions = ioConfig.getJdbcOffsets().getOffsetMaps();
    int numPartitions = (partitions != null ? partitions.size() : 0);
    log.info("Found [%d] JDBC partitions for table [%s]", numPartitions, ioConfig.getTable());

    for (int partition = 0; partition < numPartitions; partition++) {
      int taskGroupId = getTaskGroup(partition);
      groups.putIfAbsent(taskGroupId, new ConcurrentHashMap<Integer, Long>());
      Map<Integer, Long> offsetsMap = groups.get(taskGroupId);

      // The starting offset for a table in [groups] is initially set to NOT_SET; when a new task group
      // is created , if the offset in [groups] is NOT_SET it will take the starting
      // offset value from the metadata store, and if it can't find it there, from Table. Once a task begins
      // publishing, the offset in groups will be updated to the ending offset of the publishing-but-not-yet-
      // completed task, which will cause the next set of tasks to begin reading from where the previous task left
      // off. If that previous task now fails, we will set the offset in [groups] back to NOT_SET which will
      // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
      // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
      // failures during publishing.

      if (offsetsMap.putIfAbsent(partition, NOT_SET) == null) {
        log.info(
            "New  [%s] added to task group [%d]",
            ioConfig.getTable(),
            taskGroupId
        );
      }
    }
  }

  private void discoverTasks() throws ExecutionException, InterruptedException, TimeoutException {
    int taskCount = 0;
    List<String> futureTaskIds = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    List<Task> tasks = taskStorage.getActiveTasks();

    log.info("TaskStorage ActiveTasks is [%d]", tasks.size());

    for (Task task : tasks) {
      if (!(task instanceof JDBCIndexTask) || !dataSource.equals(task.getDataSource())) {
        continue;
      }

      taskCount++;
      final JDBCIndexTask jdbcTask = (JDBCIndexTask) task;
      final String taskId = task.getId();

      // Determine which task group this task belongs to based on table handled by this task. If we
      // later determine that this task is actively reading, we will make sure that it matches our current partition
      // allocation (getTaskGroup(partition) should return the same value for every partition being read
      // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
      // state, we will permit it to complete even if it doesn't match our current partition allocation to support
      // seamless schema migration.

      Iterator<Integer> it = jdbcTask.getIOConfig().getJdbcOffsets().getOffsetMaps().keySet().iterator();
      final Integer taskGroupId = (it.hasNext() ? getTaskGroup(it.next()) : null);

      log.info("taskGroupId is " + taskGroupId);

      if (taskGroupId != null) {
        // check to see if we already know about this task, either in [taskGroups] or in [pendingCompletionTaskGroups]
        // and if not add it to taskGroups or pendingCompletionTaskGroups (if status = PUBLISHING)
        TaskGroup taskGroup = taskGroups.get(taskGroupId);
        if (!isTaskInPendingCompletionGroups(taskId) && (taskGroup == null || !taskGroup.tasks.containsKey(taskId))) {
          log.info("TaskGroup info details taskId [%s] in taskGroupId [%s]", taskId, taskGroupId);
          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStatusAsync(taskId), new Function<JDBCIndexTask.Status, Boolean>() {
                    @Override
                    public Boolean apply(JDBCIndexTask.Status status) {
                      if (status == JDBCIndexTask.Status.PUBLISHING) {
                        addDiscoveredTaskToPendingCompletionTaskGroups(
                            taskGroupId,
                            taskId,
                            jdbcTask.getIOConfig()
                                .getJdbcOffsets().getOffsetMaps()
                        );

                        // update groups with the publishing task's offsets (if they are greater than what is
                        // existing) so that the next tasks will start reading from where this task left off
                        Map<Integer, Long> publishingTaskCurrentOffsets = taskClient.getCurrentOffsets(taskId, true);
                        for (Map.Entry<Integer, Long> entry : publishingTaskCurrentOffsets.entrySet()) {
                          Integer partition = entry.getKey();
                          long endOffset = entry.getValue();
                          log.info("Current offset is [%s]", endOffset);
                          ConcurrentHashMap<Integer, Long> offsetsMap = (ConcurrentHashMap<Integer, Long>) groups.get(getTaskGroup(partition));
                          boolean succeeded;
                          do {
                            succeeded = true;
                            Long previousOffset = offsetsMap.putIfAbsent(partition, endOffset);
                            if (previousOffset != null && previousOffset < endOffset) {
                              succeeded = offsetsMap.replace(partition, previousOffset, endOffset);
                            }
                          } while (!succeeded);
                        }

                      } else {
                        for (Integer partition : jdbcTask.getIOConfig()
                            .getJdbcOffsets().getOffsetMaps()
                            .keySet()) {
                          if (!taskGroupId.equals(getTaskGroup(partition))) {
                            log.warn(
                                "Stopping task [%s] which does not match the expected partition allocation",
                                taskId
                            );
                            try {
                              stopTask(taskId, false).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                              log.warn(e, "Exception while stopping task");
                            }
                            return false;
                          }

                          if (taskGroups.putIfAbsent(
                              taskGroupId,
                              new TaskGroup(
                                  ImmutableMap.copyOf(jdbcTask.getIOConfig()
                                      .getJdbcOffsets().getOffsetMaps())
                                  , jdbcTask.getIOConfig().getMinimumMessageTime()
                              )
                          ) == null) {
                            log.info("Created new task group [%d] from discoverTasks", taskGroupId);
                          }

                          if (!isTaskCurrent(taskGroupId, taskId)) {
                            log.info(
                                "Stopping task [%s] which does not match the expected parameters and ingestion spec",
                                taskId
                            );
                            try {
                              stopTask(taskId, false).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
                            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                              log.warn(e, "Exception while stopping task");
                            }
                            return false;
                          } else {
                            log.info("taskGroup put IfAbsent by [%s]", taskId);
                            taskGroups.get(taskGroupId).tasks.putIfAbsent(taskId, new TaskData());
                          }
                        }
                      }
                      return true;
                    }
                  }, workerExec
              )
          );
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        log.warn("Task [%s] failed to return status, killing task", taskId);
        killTask(taskId);
      }
    }
    log.debug("Found [%d] JDBC indexing tasks for dataSource [%s]", taskCount, dataSource);
  }

  private void addDiscoveredTaskToPendingCompletionTaskGroups(
      int groupId,
      String taskId,
      Map<Integer, Long> offsetMaps
  ) {
    pendingCompletionTaskGroups.putIfAbsent(groupId, Lists.<TaskGroup>newCopyOnWriteArrayList());

    CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingCompletionTaskGroups.get(groupId);
    for (TaskGroup taskGroup : taskGroupList) {
      log.info("TaskGroup Offset Info [%s] vs offsetMaps [%s]", taskGroup.offsetsMap, offsetMaps);
      if (taskGroup.offsetsMap.equals(offsetMaps)) {
        if (taskGroup.tasks.putIfAbsent(taskId, new TaskData()) == null) {
          log.info("Added discovered task [%s] to existing pending task group", taskId);
        }
        return;
      }
    }

    log.info("Creating new pending completion task group for discovered task [%s]", taskId);

    // reading the minimumMessageTime from the publishing task and setting it here is not necessary as this task cannot
    // change to a state where it will read any more events
    TaskGroup newTaskGroup = new TaskGroup(ImmutableMap.copyOf(offsetMaps), Optional.<DateTime>absent());

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
  }

  private void updateTaskStatus() throws ExecutionException, InterruptedException, TimeoutException {
    final List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    final List<String> futureTaskIds = Lists.newArrayList();

    // update status (and startTime if unknown) of current tasks in taskGroups
    for (TaskGroup group : taskGroups.values()) {
      for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
        final String taskId = entry.getKey();
        final TaskData taskData = entry.getValue();

        if (taskData.startTime == null) {
          futureTaskIds.add(taskId);
          futures.add(
              Futures.transform(
                  taskClient.getStartTimeAsync(taskId), new Function<DateTime, Boolean>() {
                    @Nullable
                    @Override
                    public Boolean apply(@Nullable DateTime startTime) {
                      if (startTime == null) {
                        return false;
                      }

                      taskData.startTime = startTime;
                      long millisRemaining = ioConfig.getTaskDuration().getMillis() - (System.currentTimeMillis()
                          - taskData.startTime.getMillis());
                      if (millisRemaining > 0) {
                        log.info("buildRunTask scheduled......");
                        scheduledExec.schedule(
                            buildRunTask(),
                            millisRemaining + MAX_RUN_FREQUENCY_MILLIS,
                            TimeUnit.MILLISECONDS
                        );
                      }

                      return true;
                    }
                  }, workerExec
              )
          );
        }

        taskData.status = taskStorage.getStatus(taskId).get();
      }
    }

    // update status of pending completion tasks in pendingCompletionTaskGroups
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup group : taskGroups) {
        for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
          entry.getValue().status = taskStorage.getStatus(entry.getKey()).get();
        }
      }
    }

    List<Boolean> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    for (int i = 0; i < results.size(); i++) {
      // false means the task hasn't started running yet and that's okay; null means it should be running but the HTTP
      // request threw an exception so kill the task
      if (results.get(i) == null) {
        String taskId = futureTaskIds.get(i);
        log.warn("Task [%s] failed to return start time, killing task", taskId);
        killTask(taskId);
      }
    }
  }

  private void checkTaskDuration() throws InterruptedException, ExecutionException, TimeoutException {
    final List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    final List<Integer> futureGroupIds = Lists.newArrayList();

    for (Map.Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      Integer groupId = entry.getKey();
      TaskGroup group = entry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTime.now();
      for (TaskData taskData : group.tasks.values()) {
        if (earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }

      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow()) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        futureGroupIds.add(groupId);
        futures.add(signalTasksToFinish(groupId));
      }
    }

    List<Map<Integer, Long>> results = Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
    log.info("checkTaskDuration results size is [%s]", results.size());
    log.info("checkTaskDuration results info is [%s]", results.toString());

    for (int j = 0; j < results.size(); j++) {
      Integer groupId = futureGroupIds.get(j);
      TaskGroup group = taskGroups.get(groupId);
      Map<Integer, Long> endOffsets = results.get(j);
      log.info("checkTaskDuration endOffsets is [%s]", endOffsets);

      if (endOffsets != null) {
        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());
        pendingCompletionTaskGroups.putIfAbsent(groupId, Lists.<TaskGroup>newCopyOnWriteArrayList());
        pendingCompletionTaskGroups.get(groupId).add(group);

        // set endOffsets as the next startOffsets
        for (Map.Entry<Integer, Long> entry : endOffsets.entrySet()) {
          groups.get(groupId).put(entry.getKey(), entry.getValue());
          log.info("checkTaskDuration groups info [%d], [%d]", entry.getKey(), entry.getValue());
        }
      } else {
        log.warn(
            "All tasks in group [%s] failed to transition to publishing state, killing tasks [%s]",
            groupId,
            group.taskIds()
        );
        for (String id : group.taskIds()) {
          killTask(id);
        }
      }

      // remove this task group from the list of current task groups now that it has been handled
      log.info("TaskGroups removed by [%s]", groupId);
      taskGroups.remove(groupId);
    }
  }

  private ListenableFuture<Map<Integer, Long>> signalTasksToFinish(final int groupId) {
    final TaskGroup taskGroup = taskGroups.get(groupId);

    // 1) Check if any task completed (in which case we're done) and kill unassigned tasks
    Iterator<Map.Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<String, TaskData> taskEntry = i.next();
      String taskId = taskEntry.getKey();
      TaskData task = taskEntry.getValue();

      if (task.status.isSuccess()) {
        // If any task in this group has already completed, stop the rest of the tasks in the group and return.
        // This will cause us to create a new set of tasks next cycle that will start from the offsets in
        // metadata store (which will have advanced if we succeeded in publishing and will remain the same if publishing
        // failed and we need to re-ingest)
        return Futures.transform(
            stopTasksInGroup(taskGroup), new Function<Object, Map<Integer, Long>>() {
              @Nullable
              @Override
              public Map<Integer, Long> apply(@Nullable Object input) {
                return null;
              }
            }
        );
      }

      if (task.status.isRunnable()) {
        if (taskInfoProvider.getTaskLocation(taskId).equals(TaskLocation.unknown())) {
          log.info("Killing task [%s] which hasn't been assigned to a worker", taskId);
          killTask(taskId);
          i.remove();
        }
      }
    }

    // 2) Pause running tasks
    final List<ListenableFuture<Map<Integer, Long>>> pauseFutures = Lists.newArrayList();
    final List<String> pauseTaskIds = ImmutableList.copyOf(taskGroup.taskIds());
    for (final String taskId : pauseTaskIds) {
      log.info("taskClient pauseAsync called by [%s]", taskId);
      pauseFutures.add(taskClient.pauseAsync(taskId));
    }

    return Futures.transform(
        Futures.successfulAsList(pauseFutures), new Function<List<Map<Integer, Long>>, Map<Integer, Long>>() {
          @Nullable
          @Override
          public Map<Integer, Long> apply(List<Map<Integer, Long>> input) {
            // 3) Build a map of the highest offset read by any task in the group for each partition
            final Map<Integer, Long> endOffsets = new HashMap<>();
            for (int i = 0; i < input.size(); i++) {
              Map<Integer, Long> result = input.get(i);

              if (result == null) { // kill tasks that didn't return a value
                String taskId = pauseTaskIds.get(i);
                log.warn("Task [%s] failed to respond to [pause] in a timely manner, killing task", taskId);
                killTask(taskId);
                taskGroup.tasks.remove(taskId);

              } else { // otherwise build a map of the highest offsets seen
                for (Map.Entry<Integer, Long> offset : result.entrySet()) {
                  if (!endOffsets.containsKey(offset.getKey())
                      || endOffsets.get(offset.getKey()).compareTo(offset.getValue()) < 0) {
                    endOffsets.put(offset.getKey(), offset.getValue());
                  }
                }
              }
            }

            // 4) Set the end offsets for each task to the values from step 3 and resume the tasks. All the tasks should
            //    finish reading and start publishing within a short period, depending on how in sync the tasks were.
            final List<ListenableFuture<Boolean>> setEndOffsetFutures = Lists.newArrayList();
            final List<String> setEndOffsetTaskIds = ImmutableList.copyOf(taskGroup.taskIds());

            if (setEndOffsetTaskIds.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", groupId);
              return null;
            }

            log.info("Setting endOffsets for tasks in taskGroup [%d] to %s and resuming", groupId, endOffsets);
            for (final String taskId : setEndOffsetTaskIds) {
              setEndOffsetFutures.add(taskClient.setEndOffsetsAsync(taskId, endOffsets, true));
            }

            try {
              List<Boolean> results = Futures.successfulAsList(setEndOffsetFutures)
                  .get(futureTimeoutInSeconds, TimeUnit.SECONDS);
              for (int i = 0; i < results.size(); i++) {
                if (results.get(i) == null || !results.get(i)) {
                  String taskId = setEndOffsetTaskIds.get(i);
                  log.warn("Task [%s] failed to respond to [set end offsets] in a timely manner, killing task", taskId);
                  killTask(taskId);
                  taskGroup.tasks.remove(taskId);
                }
              }
            } catch (Exception e) {
              Throwables.propagate(e);
            }

            if (taskGroup.tasks.isEmpty()) {
              log.info("All tasks in taskGroup [%d] have failed, tasks will be re-created", groupId);
              return null;
            }
            return endOffsets;
          }
        }, workerExec
    );
  }

  /**
   * Monitors [pendingCompletionTaskGroups] for tasks that have completed. If any task in a task group has completed, we
   * can safely stop the rest of the tasks in that group. If a task group has exceeded its publishing timeout, then
   * we need to stop all tasks in not only that task group but also 1) any subsequent task group that is also pending
   * completion and 2) the current task group that is running, because the assumption that we have handled up to the
   * starting offset for subsequent task groups is no longer valid, and subsequent tasks would fail as soon as they
   * attempted to publish because of the contiguous range consistency check.
   */
  private void checkPendingCompletionTasks() throws ExecutionException, InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = Lists.newArrayList();

    for (Map.Entry<Integer, CopyOnWriteArrayList<TaskGroup>> pendingGroupList : pendingCompletionTaskGroups.entrySet()) {

      boolean stopTasksInTaskGroup = false;
      Integer groupId = pendingGroupList.getKey();
      CopyOnWriteArrayList<TaskGroup> taskGroupList = pendingGroupList.getValue();
      List<TaskGroup> toRemove = Lists.newArrayList();

      for (TaskGroup group : taskGroupList) {
        boolean foundSuccess = false, entireTaskGroupFailed = false;

        if (stopTasksInTaskGroup) {
          // One of the earlier groups that was handling the same partition set timed out before the segments were
          // published so stop any additional groups handling the same partition set that are pending completion.
          futures.add(stopTasksInGroup(group));
          toRemove.add(group);
          continue;
        }

        Iterator<Map.Entry<String, TaskData>> iTask = group.tasks.entrySet().iterator();
        while (iTask.hasNext()) {
          Map.Entry<String, TaskData> task = iTask.next();

          if (task.getValue().status.isFailure()) {
            iTask.remove(); // remove failed task
            if (group.tasks.isEmpty()) {
              // if all tasks in the group have failed, just nuke all task groups with this partition set and restart
              entireTaskGroupFailed = true;
              break;
            }
          }

          if (task.getValue().status.isSuccess()) {
            // If one of the pending completion tasks was successful, stop the rest of the tasks in the group as
            // we no longer need them to publish their segment.
            log.info("Task [%s] completed successfully, stopping tasks %s", task.getKey(), group.taskIds());
            futures.add(stopTasksInGroup(group));
            foundSuccess = true;
            toRemove.add(group); // remove the TaskGroup from the list of pending completion task groups
            break; // skip iterating the rest of the tasks in this group as they've all been stopped now
          }
        }

        if ((!foundSuccess && group.completionTimeout.isBeforeNow()) || entireTaskGroupFailed) {
          if (entireTaskGroupFailed) {
            log.warn("All tasks in group [%d] failed to publish, killing all tasks for these partitions", groupId);
          } else {
            log.makeAlert(
                "No task in [%s] succeeded before the completion timeout elapsed [%s]!",
                group.taskIds(),
                ioConfig.getCompletionTimeout()
            ).emit();
          }

          // reset partitions offsets for this task group so that they will be re-read from metadata storage
          groups.remove(groupId);

          // stop all the tasks in this pending completion group
          futures.add(stopTasksInGroup(group));

          // set a flag so the other pending completion groups for this set of partitions will also stop
          stopTasksInTaskGroup = true;

          // stop all the tasks in the currently reading task group and remove the bad task group
          futures.add(stopTasksInGroup(taskGroups.remove(groupId)));

          toRemove.add(group);
        }
      }

      taskGroupList.removeAll(toRemove);
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  private void checkCurrentTaskState() throws ExecutionException, InterruptedException, TimeoutException {
    List<ListenableFuture<?>> futures = Lists.newArrayList();
    Iterator<Map.Entry<Integer, TaskGroup>> iTaskGroups = taskGroups.entrySet().iterator();
    while (iTaskGroups.hasNext()) {
      Map.Entry<Integer, TaskGroup> taskGroupEntry = iTaskGroups.next();
      Integer groupId = taskGroupEntry.getKey();
      TaskGroup taskGroup = taskGroupEntry.getValue();

      // Iterate the list of known tasks in this group and:
      //   1) Kill any tasks which are not "current" (have the partitions, starting offsets, and minimumMessageTime
      //      (if applicable) in [taskGroups])
      //   2) Remove any tasks that have failed from the list
      //   3) If any task completed successfully, stop all the tasks in this group and move to the next group

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.taskIds());

      Iterator<Map.Entry<String, TaskData>> iTasks = taskGroup.tasks.entrySet().iterator();
      while (iTasks.hasNext()) {
        Map.Entry<String, TaskData> task = iTasks.next();
        String taskId = task.getKey();
        TaskData taskData = task.getValue();

        // stop and remove bad tasks from the task group
        if (!isTaskCurrent(groupId, taskId)) {
          log.info("Stopping task [%s] which does not match the expected offset range and ingestion spec", taskId);
          futures.add(stopTask(taskId, false));
          iTasks.remove();
          continue;
        }

        // remove failed tasks
        if (taskData.status.isFailure()) {
          iTasks.remove();
          continue;
        }

        // check for successful tasks, and if we find one, stop all tasks in the group and remove the group so it can
        // be recreated with the next set of offsets
        if (taskData.status.isSuccess()) {
          futures.add(stopTasksInGroup(taskGroup));
          iTaskGroups.remove();
          break;
        }
      }
      log.debug("Task group [%d] post-pruning: %s", groupId, taskGroup.taskIds());
    }

    // wait for all task shutdowns to complete before returning
    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  void createNewTasks() throws IOException {
    // check that there is a current task group for each group of partitions in [groups]
    for (Integer groupId : groups.keySet()) {
      if (!taskGroups.containsKey(groupId)) {
        log.info("Creating new task group [%d] for partitions %s", groupId, groups.get(groupId).keySet());

        Optional<DateTime> minimumMessageTime = (ioConfig.getLateMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTime.now().minus(ioConfig.getLateMessageRejectionPeriod().get())
        ) : Optional.<DateTime>absent());

        taskGroups.put(groupId, new TaskGroup(generateStartingOffsetsForGroup(groupId), minimumMessageTime));
      }
    }

    // iterate through all the current task groups and make sure each one has the desired number of replica tasks
    boolean createdTask = false;

    log.info("Current number of task Groups [%d], createdTask status is [%b]", taskGroups.size(), createdTask);
    for (Map.Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();
      log.info("taskGroup.tasks.size() is " + taskGroup.tasks.size());

      if (ioConfig.getReplicas() > taskGroup.tasks.size() && taskGroup.offsetsMap != null) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks, offsetMap is [%s]",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId, taskGroup.offsetsMap
        );
        createJDBCTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      log.info("Create New Tasks....");
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }
  }

  private ImmutableMap<Integer, Long> generateStartingOffsetsForGroup(int groupId) {
    ImmutableMap.Builder<Integer, Long> builder = ImmutableMap.builder();
    for (Map.Entry<Integer, Long> entry : groups.get(groupId).entrySet()) {
      Integer partition = entry.getKey();
      Long offset = entry.getValue();
      if (offset != null && offset != NOT_SET) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        builder.put(partition, offset);
      } else {
        builder.put(partition, getOffsetFromStorage());
      }
    }
    return builder.build();
  }


  private long getOffsetFromStorage() {
    long offset;
    Map<Integer, Long> metadataOffsets = getOffsetsFromMetadataStorage();
    if (!metadataOffsets.isEmpty()) {
      log.info("Getting offset [%s] from metadata storage ", metadataOffsets);
      offset = (long)metadataOffsets.values().toArray()[0];
    } else { //If there is no metadata from storage, set the initial config value.
      log.info("Getting offset [%s] from io configuration", ioConfig.getJdbcOffsets().getOffsetMaps());
      offset = (long) ioConfig.getJdbcOffsets().getOffsetMaps().values().toArray()[0];
    }
    return offset;
  }

  private Map<Integer, Long> getOffsetsFromMetadataStorage() {
    DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata != null && dataSourceMetadata instanceof JDBCDataSourceMetadata) {
      JDBCOffsets jdbcOffsets = ((JDBCDataSourceMetadata) dataSourceMetadata).getJdbcOffsets();
      if (jdbcOffsets != null) {
        if (!ioConfig.getTable().equals(jdbcOffsets.getTable())) {
          log.warn(
              "Table in metadata storage [%s] doesn't match spec table [%s], ignoring stored offsets",
              jdbcOffsets.getTable(),
              ioConfig.getTable()
          );
          return ImmutableMap.of();
        } else if (jdbcOffsets.getOffsetMaps() != null) {
          return jdbcOffsets.getOffsetMaps();
        }
      }
    }
    return ImmutableMap.of();
  }


  private void createJDBCTasksForGroup(int groupId, int replicas) throws IOException

  {
    log.info("CreateJDBCTasksForGroup by [%s] and Offsets = [%s]", groupId, taskGroups.get(groupId).offsetsMap);
    Map<Integer, Long> offsetMaps = taskGroups.get(groupId).offsetsMap;
    String sequenceName = generateSequenceName(groupId);
    DateTime minimumMessageTime = taskGroups.get(groupId).minimumMessageTime.orNull();

    JDBCIOConfig jdbcIOConfig = new JDBCIOConfig(
        sequenceName,
        ioConfig.getTable(),
        ioConfig.getUser(),
        ioConfig.getPassword(),
        ioConfig.getConnectURI(),
        ioConfig.getDriverClass(),
        new JDBCOffsets(ioConfig.getTable(), offsetMaps),
        true,
        false,
        minimumMessageTime,
        ioConfig.getQuery(),
        ioConfig.getColumns(),
        ioConfig.getInterval()
    );

    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(sequenceName, getRandomId());
      JDBCIndexTask indexTask = new JDBCIndexTask(
          taskId,
          new TaskResource(sequenceName, 1),
          spec.getDataSchema(),
          taskTuningConfig,
          jdbcIOConfig,
          spec.getContext(),
          null
      );

      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        try {
          taskQueue.get().add(indexTask);
        } catch (EntryExistsException e) {
          log.error("Tried to add task [%s] but it already exists", indexTask.getId());
        }
      } else {
        log.error("Failed to get task queue because I'm not the leader!");
      }
    }
  }


  /**
   * Compares the sequence name from the task with one generated for the task's group ID and returns false if they do
   * not match. The sequence name is generated from a hash of the dataSchema, tuningConfig, starting offsets, and the
   * minimumMessageTime if set.
   */
  private boolean isTaskCurrent(int taskGroupId, String taskId) {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !(taskOptional.get() instanceof JDBCIndexTask)) {
      return false;
    }

    String taskSequenceName = ((JDBCIndexTask) taskOptional.get()).getIOConfig().getBaseSequenceName();

    return generateSequenceName(taskGroupId).equals(taskSequenceName);
  }

  private ListenableFuture<?> stopTasksInGroup(TaskGroup taskGroup) {
    if (taskGroup == null) {
      return Futures.immediateFuture(null);
    }

    final List<ListenableFuture<Void>> futures = Lists.newArrayList();
    for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
      if (!entry.getValue().status.isComplete()) {
        futures.add(stopTask(entry.getKey(), false));
      }
    }

    return Futures.successfulAsList(futures);
  }

  private ListenableFuture<Void> stopTask(final String id, final boolean publish) {
    return Futures.transform(
        taskClient.stopAsync(id, publish), new Function<Boolean, Void>() {
          @Nullable
          @Override
          public Void apply(@Nullable Boolean result) {
            if (result == null || !result) {
              log.info("Task [%s] failed to stop in a timely manner, killing task", id);
              killTask(id);
            }
            return null;
          }
        }
    );
  }

  private void killTask(final String id) {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdown(id);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private int getTaskGroup(int partition) {
    return partition % ioConfig.getTaskCount();
  }

  private boolean isTaskInPendingCompletionGroups(String taskId) {
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup taskGroup : taskGroups) {
        if (taskGroup.tasks.containsKey(taskId)) {
          return true;
        }
      }
    }
    return false;
  }

  private JDBCSupervisorReport generateReport(boolean includeOffsets) {
    int numPartitions = (int) groups.values().stream().count();
    Map<Integer, Long> lag = getLag(getHighestCurrentOffsets());
    JDBCSupervisorReport report = new JDBCSupervisorReport(
        dataSource,
        DateTime.now(),
        ioConfig.getTable(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000,
        includeOffsets ? latestOffsetsFromJDBC : null,
        includeOffsets ? lag : null,
        includeOffsets ? lag.values().stream().mapToLong(x -> Math.max(x, 0)).max().getAsLong() : null,
        includeOffsets ? offsetsLastUpdated : null
    );

    List<TaskReportData> taskReports = Lists.newArrayList();

    try {
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          DateTime startTime = entry.getValue().startTime;
          Map<Integer, Long> currentOffsets = entry.getValue().currentOffsets;
          Long remainingSeconds = null;
          if (startTime != null) {
            remainingSeconds = Math.max(
                0, ioConfig.getTaskDuration().getMillis() - (DateTime.now().getMillis() - startTime.getMillis())
            ) / 1000;
          }

          taskReports.add(
              new TaskReportData(
                  taskId,
                  includeOffsets ? taskGroup.offsetsMap : null,
//                  includeOffsets ? currentOffsets: null,
                  startTime,
                  remainingSeconds,
                  TaskReportData.TaskType.ACTIVE,
                  includeOffsets ? getLag(currentOffsets) : null //TODO::: check value
              )
          );
        }
      }

      for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
        for (TaskGroup taskGroup : taskGroups) {
          for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
            String taskId = entry.getKey();
            DateTime startTime = entry.getValue().startTime;
            Map<Integer, Long> currentOffsets = entry.getValue().currentOffsets;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - DateTime.now().getMillis())
                  / 1000;
            }

            taskReports.add(
                new TaskReportData(
                    taskId,
                    includeOffsets ? taskGroup.offsetsMap : null,
//                    includeOffsets ? currentOffsets: null,
                    startTime,
                    remainingSeconds,
                    TaskReportData.TaskType.PUBLISHING,
                    null
                )
            );
          }
        }
      }

      taskReports.stream().forEach(report::addTask);
    } catch (Exception e) {
      log.warn(e, "Failed to generate status report");
    }

    return report;
  }

  private Runnable buildRunTask() {
    log.info("BuildRunTask.....");
    return () -> notices.add(new RunNotice());
  }

  private Map<Integer, Long> getHighestCurrentOffsets() {

//    return taskGroups
//          .values()
//          .stream()
//          .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
//          .flatMap(taskData -> taskData.getValue().currentOffsets.entrySet().stream())
//          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::max));
    return taskGroups.get(getTaskGroup(0)).offsetsMap;
  }

  private Map<Integer, Long> getLag(Map<Integer, Long> currentOffsets) {
//    return currentOffsets != null && latestOffsetsFromJDBC != null ? currentOffsets
//        .entrySet()
//        .stream()
//        .collect(
//            Collectors.toMap(
//                Map.Entry::getKey,
//                e -> latestOffsetsFromJDBC != null
//                    && latestOffsetsFromJDBC.get(e.getKey()) != null
//                    && e.getValue() != null
//                    ? latestOffsetsFromJDBC.get(e.getKey()) - e.getValue()
//                    : null
//            )
//        ) : currentOffsets;
    return currentOffsets;
  }

  private Runnable emitLag() {
    return () -> {
      try {
        Map<Integer, Long> highestCurrentOffsets = getHighestCurrentOffsets();
        long lag = getLag(highestCurrentOffsets).values().stream().mapToLong(x -> Math.max(x, 0)).max().getAsLong();
        emitter.emit(
            ServiceMetricEvent.builder().setDimension("dataSource", dataSource).build("ingest/jdbc/lag", lag)
        );
      } catch (Exception e) {
        log.warn(e, "Unable to compute JDBC lag");
      }
    };
  }

  private void updateLatestOffsetsFromJDBC() throws IOException {
    latestOffsetsFromJDBC = getHighestCurrentOffsets() != null ? getHighestCurrentOffsets() : ioConfig.getJdbcOffsets().getOffsetMaps();
    //TODO::: update offset info
  }


  private void updateCurrentOffsets() throws InterruptedException, ExecutionException, TimeoutException {
    log.info("updateCurrentOffsets called");
    final List<ListenableFuture<Void>> futures = Stream.concat(
        taskGroups.values().stream().flatMap(taskGroup -> taskGroup.tasks.entrySet().stream()),
        pendingCompletionTaskGroups.values()
            .stream()
            .flatMap(List::stream)
            .flatMap(taskGroup -> taskGroup.tasks.entrySet().stream())
    ).map(
        task -> Futures.transform(
            taskClient.getCurrentOffsetsAsync(task.getKey(), false),
            (Function<Map<Integer, Long>, Void>) (currentOffsets) -> {
              log.info("TaskClient currentOffsets is [%s]", currentOffsets);
              if (currentOffsets != null && !currentOffsets.isEmpty()) {
                task.getValue().currentOffsets = currentOffsets;
                log.info("task.getValue().currentOffsets  is " + task.getValue().currentOffsets);
              } else {

              }
              return null;
            }
        )
    ).collect(Collectors.toList());
    log.info("CurrentOffsets size is " + taskGroups.values().size());

    Futures.successfulAsList(futures).get(futureTimeoutInSeconds, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  Runnable updateCurrentAndLatestOffsets() {
    return () -> {
      try {
        updateCurrentOffsets();
        updateLatestOffsetsFromJDBC();
        offsetsLastUpdated = DateTime.now();
      } catch (Exception e) {
        log.warn(e, "Exception while getting current/latest offsets");
      }
    };
  }
}
