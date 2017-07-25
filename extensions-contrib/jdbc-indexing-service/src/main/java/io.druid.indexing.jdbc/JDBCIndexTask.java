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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.indexing.appenderator.ActionBasedSegmentAllocator;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.AbstractTask;
import io.druid.indexing.common.task.TaskResource;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.DruidMetrics;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.RealtimeMetricsMonitor;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorDriver;
import io.druid.segment.realtime.appenderator.AppenderatorDriverAddResult;
import io.druid.segment.realtime.appenderator.Appenderators;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.timeline.DataSegment;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;

public class JDBCIndexTask extends AbstractTask implements ChatHandler
{
  public static final long PAUSE_FOREVER = -1L;
  private static final EmittingLogger log = new EmittingLogger(JDBCIndexTask.class);
  private static final String TYPE = "index_jdbc";
  private static final Random RANDOM = new Random();
  private static final long POLL_TIMEOUT = 100;
  private static final long LOCK_ACQUIRE_TIMEOUT_SECONDS = 15;
  private static final String METADATA_NEXT_PARTITIONS = "nextPartitions";
  private final DataSchema dataSchema;
  private final MapInputRowParser parser;
  private final JDBCTuningConfig tuningConfig;
  private final JDBCIOConfig ioConfig;
  private final Optional<ChatHandlerProvider> chatHandlerProvider;
  private final String DEFAULT_NULLSTRING = "";
  private final float DEFAULT_NULLNUMERIC = 0;
  private final Lock pauseLock = new ReentrantLock();
  private final Condition hasPaused = pauseLock.newCondition();
  private final Condition shouldResume = pauseLock.newCondition();
  // [pollRetryLock] and [isAwaitingRetry] is used when the Kafka consumer returns an OffsetOutOfRangeException and we
  // pause polling from Kafka for POLL_RETRY_MS before trying again. This allows us to signal the sleeping thread and
  // resume the main run loop in the case of a pause or stop request from a Jetty thread.
  private final Lock pollRetryLock = new ReentrantLock();
  private final Condition isAwaitingRetry = pollRetryLock.newCondition();
  // [statusLock] is used to synchronize the Jetty thread calling stopGracefully() with the main run thread. It prevents
  // the main run thread from switching into a publishing state while the stopGracefully() thread thinks it's still in
  // a pre-publishing state. This is important because stopGracefully() will try to use the [stopRequested] flag to stop
  // the main thread where possible, but this flag is not honored once publishing has begun so in this case we must
  // interrupt the thread. The lock ensures that if the run thread is about to transition into publishing state, it
  // blocks until after stopGracefully() has set [stopRequested] and then does a final check on [stopRequested] before
  // transitioning to publishing state.
  private final Object statusLock = new Object();
  private Integer endOffsets = 0;
  private Integer nextOffsets = 0;
  private ObjectMapper mapper;
  private volatile Appenderator appenderator = null;
  private volatile FireDepartmentMetrics fireDepartmentMetrics = null;

  // The pause lock and associated conditions are to support coordination between the Jetty threads and the main
  // ingestion loop. The goal is to provide callers of the API a guarantee that if pause() returns successfully
  // the ingestion loop has been stopped at the returned offsets and will not ingest any more data until resumed. The
  // fields are used as follows (every step requires acquiring [pauseLock]):
  //   Pausing:
  //   - In pause(), [pauseRequested] is set to true and then execution waits for [status] to change to PAUSED, with the
  //     condition checked when [hasPaused] is signalled.
  //   - In possiblyPause() called from the main loop, if [pauseRequested] is true, [status] is set to PAUSED,
  //     [hasPaused] is signalled, and execution pauses until [pauseRequested] becomes false, either by being set or by
  //     the [pauseMillis] timeout elapsing. [pauseRequested] is checked when [shouldResume] is signalled.
  //   Resuming:
  //   - In resume(), [pauseRequested] is set to false, [shouldResume] is signalled, and execution waits for [status] to
  //     change to something other than PAUSED, with the condition checked when [shouldResume] is signalled.
  //   - In possiblyPause(), when [shouldResume] is signalled, if [pauseRequested] has become false the pause loop ends,
  //     [status] is changed to STARTING and [shouldResume] is signalled.
  private volatile DateTime startTime;
  private volatile Status status = Status.NOT_STARTED; // this is only ever set by the task runner thread (runThread)
  private volatile Thread runThread = null;
  private volatile boolean stopRequested = false;
  private volatile boolean publishOnStop = false;
  private volatile boolean pauseRequested = false;
  private volatile long pauseMillis = 0;
  // This value can be tuned in some tests
  private long pollRetryMs = 30000;

  @JsonCreator
  public JDBCIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") JDBCTuningConfig tuningConfig,
      @JsonProperty("ioConfig") JDBCIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ChatHandlerProvider chatHandlerProvider
  )
  {
    super(
        id == null ? makeTaskId(dataSchema.getDataSource(), RANDOM.nextInt()) : id,
        String.format("%s_%s", TYPE, dataSchema.getDataSource()),
        taskResource,
        dataSchema.getDataSource(),
        context
    );

    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.parser = new MapInputRowParser(dataSchema.getParser().getParseSpec());
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
    this.ioConfig = Preconditions.checkNotNull(ioConfig, "ioConfig");
    this.chatHandlerProvider = Optional.fromNullable(chatHandlerProvider);

  }

  private static String makeTaskId(String dataSource, int randomBits)
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((randomBits >>> (i * 4)) & 0x0F)));
    }
    return Joiner.on("_").join(TYPE, dataSource, suffix);
  }

  @VisibleForTesting
  void setPollRetryMs(long retryMs)
  {
    this.pollRetryMs = retryMs;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public JDBCTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty("ioConfig")
  public JDBCIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    log.info("Starting up!");
    startTime = DateTime.now();
    mapper = toolbox.getObjectMapper();
    status = Status.STARTING;

    if (chatHandlerProvider.isPresent()) {
      log.info("Found chat handler of class[%s]", chatHandlerProvider.get().getClass().getName());
      chatHandlerProvider.get().register(getId(), this, false);
    } else {
      log.warn("No chat handler detected");
    }

    runThread = Thread.currentThread();

    // Set up FireDepartmentMetrics
    final FireDepartment fireDepartmentForMetrics = new FireDepartment(
        dataSchema,
        new RealtimeIOConfig(null, null, null),
        null
    );
    fireDepartmentMetrics = fireDepartmentForMetrics.getMetrics();
    toolbox.getMonitorScheduler().addMonitor(
        new RealtimeMetricsMonitor(
            ImmutableList.of(fireDepartmentForMetrics),
            ImmutableMap.of(DruidMetrics.TASK_ID, new String[]{getId()})
        )
    );

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(ioConfig.getUser());
    dataSource.setPassword(ioConfig.getPassword());
    dataSource.setUrl(ioConfig.getConnectURI());
    dataSource.setDriverClassLoader(getClass().getClassLoader());

    final String table = ioConfig.getTableName();

    if (!StringUtils.isEmpty(ioConfig.getDriverClass())) {
      dataSource.setDriverClassName(ioConfig.getDriverClass());
    }

    final Handle handle = new DBI(dataSource).open();
    try (
        final Appenderator appenderator0 = newAppenderator(fireDepartmentMetrics, toolbox);
        final AppenderatorDriver driver = newDriver(appenderator0, toolbox, fireDepartmentMetrics)
    ) {
      toolbox.getDataSegmentServerAnnouncer().announce();
      appenderator = appenderator0;

      // Start up, set up initial offsets.
      final Object restoredMetadata = driver.startJob();
      if (restoredMetadata == null) {
        nextOffsets = ioConfig.getPartitions().getStartOffset();
      } else {
        final Map<String, Object> restoredMetadataMap = (Map) restoredMetadata;
        final JDBCPartitions restoredNextPartitions = toolbox.getObjectMapper().convertValue(
            restoredMetadataMap.get(METADATA_NEXT_PARTITIONS),
            JDBCPartitions.class
        );
        nextOffsets = restoredNextPartitions.getEndOffset();


        // Sanity checks.
        if (!restoredNextPartitions.getTable().equals(ioConfig.getTableName())) {
          throw new ISE(
              "WTF?! Restored table[%s] but expected table[%s]",
              restoredNextPartitions.getTable(),
              ioConfig.getTableName()
          );
        }

        if (!nextOffsets.equals(ioConfig.getPartitions().getStartOffset())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              nextOffsets,
              ioConfig.getPartitions().getStartOffset()
          );
        }


        if (!nextOffsets.equals(ioConfig.getPartitions().getStartOffset())) {
          throw new ISE(
              "WTF?! Restored partitions[%s] but expected partitions[%s]",
              nextOffsets,
              ioConfig.getPartitions().getStartOffset()
          );
        }
      }


      // Set up sequenceNames.
      final Map<Integer, String> sequenceNames = Maps.newHashMap();
      sequenceNames.put(nextOffsets, String.format("%s_%s", ioConfig.getBaseSequenceName(), nextOffsets));

      // Set up committer.
      final Supplier<Committer> committerSupplier = new Supplier<Committer>()
      {
        @Override
        public Committer get()
        {
          final Integer snapshot = nextOffsets;

          return new Committer()
          {
            @Override
            public Object getMetadata()
            {
              return ImmutableMap.of(
                  METADATA_NEXT_PARTITIONS, new JDBCPartitions(
                      ioConfig.getPartitions().getTable(),
                      snapshot,
                      snapshot + snapshot - ioConfig.getPartitions().getStartOffset()
                  )
              );

            }

            @Override
            public void run()
            {
              // Do nothing.
            }
          };
        }
      };

      status = Status.READING;
      try {
        Integer assignment = nextOffsets;
        boolean stillReading = !assignment.equals(0);
        final String query = (ioConfig.getQuery() != null) ? ioConfig.getQuery() : makeQuery(ioConfig.getColumns(), ioConfig.getPartitions());
        org.skife.jdbi.v2.Query<Map<String, Object>> dbiQuery = handle.createQuery(query);

        final ResultIterator<InputRow> rowIterator = dbiQuery.map(
            new ResultSetMapper<InputRow>()
            {
              List<String> queryColumns = (ioConfig.getColumns() == null)
                                          ? Lists.<String>newArrayList()
                                          : ioConfig.getColumns();
              List<Boolean> columnIsNumeric = Lists.newArrayList();

              @Override
              public InputRow map(
                  final int index,
                  final ResultSet r,
                  final StatementContext ctx
              ) throws SQLException
              {
                try {
                  if (queryColumns.size() == 0) {
                    ResultSetMetaData metadata = r.getMetaData();
                    for (int idx = 1; idx <= metadata.getColumnCount(); idx++) {
                      queryColumns.add(metadata.getColumnName(idx));
                    }
                    Preconditions.checkArgument(
                        queryColumns.size() > 0,
                        String.format("No column in table [%s]", table)
                    );
                    verifyParserSpec(parser.getParseSpec(), queryColumns);
                  }
                  if (columnIsNumeric.size() == 0) {
                    ResultSetMetaData metadata = r.getMetaData();
                    Preconditions.checkArgument(
                        metadata.getColumnCount() >= queryColumns.size(),
                        String.format(
                            "number of column names [%d] exceeds the actual number of returning column values [%d]",
                            queryColumns.size(),
                            metadata.getColumnCount()
                        )
                    );
                    columnIsNumeric.add(false); // dummy to make start index to 1
                    for (int idx = 1; idx <= metadata.getColumnCount(); idx++) {
                      boolean isNumeric = false;
                      int type = metadata.getColumnType(idx);
                      switch (type) {
                        case BIGINT:
                        case DECIMAL:
                        case DOUBLE:
                        case FLOAT:
                        case INTEGER:
                        case NUMERIC:
                        case SMALLINT:
                        case TINYINT:
                          isNumeric = true;
                          break;
                      }
                      columnIsNumeric.add(isNumeric);
                    }
                  }
                  final Map<String, Object> columnMap = Maps.newHashMap();
                  int columnIdx = 1;
                  for (String column : queryColumns) {
                    Object objToPut = null;
                    if (table != null) {
                      objToPut = r.getObject(column);
                    } else {
                      objToPut = r.getObject(columnIdx);
                    }
                    columnMap.put(
                        column,
                        objToPut == null ? columnIsNumeric.get(columnIdx) : objToPut
                    );

                    columnIdx++;
                  }
                  return parser.parse(columnMap);

                }
                catch (IllegalArgumentException e) {
                  throw new SQLException(e);
                }
              }
            }
        ).iterator();

        final String sequenceName = sequenceNames.get(nextOffsets); //TODO::: check data

        while (rowIterator.hasNext()) {
          InputRow row = rowIterator.next();
          try {
            if (!ioConfig.getMinimumMessageTime().isPresent() ||
                !ioConfig.getMinimumMessageTime().get().isAfter(row.getTimestamp())) {

              final AppenderatorDriverAddResult addResult = driver.add(
                  row,
                  sequenceName,
                  committerSupplier
              );

              if (addResult.isOk()) {
                // If the number of rows in the segment exceeds the threshold after adding a row,
                // move the segment out from the active segments of AppenderatorDriver to make a new segment.
                if (addResult.getNumRowsInSegment() > tuningConfig.getMaxRowsPerSegment()) {
                  driver.moveSegmentOut(sequenceName, ImmutableList.of(addResult.getSegmentIdentifier()));
                }
              } else {
                // Failure to allocate segment puts determinism at risk, bail out to be safe.
                // May want configurable behavior here at some point.
                // If we allow continuing, then consider blacklisting the interval for a while to avoid constant checks.
                throw new ISE("Could not allocate segment for row with timestamp[%s]", row.getTimestamp());
              }

              fireDepartmentMetrics.incrementProcessed();
            } else {
              fireDepartmentMetrics.incrementThrownAway();
            }

          }
          catch (ParseException e) {
            if (tuningConfig.isReportParseExceptions()) {
              throw e;
            } else {
              log.debug(
                  e,
                  "Dropping unparseable row from row[%d] .",
                  row
              );

              fireDepartmentMetrics.incrementUnparseable();
            }
          }
          nextOffsets = (getCurrentOffsets() + 1);

        }


        //TODO::: query


      }
      finally {
        driver.persist(committerSupplier.get()); // persist pending data
      }
      synchronized (statusLock) {
        if (stopRequested && !publishOnStop) {
          throw new InterruptedException("Stopping without publishing");
        }

        status = Status.PUBLISHING;
      }

      final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {

        final JDBCPartitions finalPartitions = toolbox.getObjectMapper().convertValue(
            ((Map) commitMetadata).get(METADATA_NEXT_PARTITIONS),
            JDBCPartitions.class
        );
        // Sanity check, we should only be publishing things that match our desired end state.
//        if (!endOffsets.equals(finalPartitions.getOffset())) {
//          throw new ISE("WTF?! Driver attempted to publish invalid metadata[%s].", commitMetadata);
//        }

        final SegmentTransactionalInsertAction action;

        if (ioConfig.isUseTransaction()) {
          action = new SegmentTransactionalInsertAction(
              segments,
              new JDBCDataSourceMetadata(ioConfig.getTableName(), ioConfig.getPartitions().getStartOffset(), ioConfig.getPartitions().getEndOffset()),
              new JDBCDataSourceMetadata(ioConfig.getTableName(), endOffsets, endOffsets + endOffsets - ioConfig.getPartitions().getStartOffset())
          );
        } else {
          action = new SegmentTransactionalInsertAction(segments, null, null);
        }

        log.info("Publishing with isTransaction[%s].", ioConfig.isUseTransaction());

        return toolbox.getTaskActionClient().submit(action).isSuccess();
      };

      // Supervised kafka tasks are killed by JDBCSupervisor if they are stuck during publishing segments or waiting
      // for hand off. See JDBCSupervisorIOConfig.completionTimeout.
      final SegmentsAndMetadata published = driver.publish(
          publisher,
          committerSupplier.get(),
          sequenceNames.values()
      ).get();

      final SegmentsAndMetadata handedOff;
      if (tuningConfig.getHandoffConditionTimeout() == 0) {
        handedOff = driver.registerHandoff(published)
                          .get();
      } else {
        handedOff = driver.registerHandoff(published)
                          .get(tuningConfig.getHandoffConditionTimeout(), TimeUnit.MILLISECONDS);
      }

      if (handedOff == null) {
        throw new ISE("Transaction failure publishing segments, aborting");
      } else {
        log.info(
            "Published segments[%s] with metadata[%s].",
            Joiner.on(", ").join(
                Iterables.transform(
                    handedOff.getSegments(),
                    new Function<DataSegment, String>()
                    {
                      @Override
                      public String apply(DataSegment input)
                      {
                        return input.getIdentifier();
                      }
                    }
                )
            ),
            handedOff.getCommitMetadata()
        );
      }

    }

    catch (InterruptedException |
        RejectedExecutionException e
        )

    {
      // handle the InterruptedException that gets wrapped in a RejectedExecutionException
      if (e instanceof RejectedExecutionException
          && (e.getCause() == null || !(e.getCause() instanceof InterruptedException))) {
        throw e;
      }

      // if we were interrupted because we were asked to stop, handle the exception and return success, else rethrow
      if (!stopRequested) {
        Thread.currentThread().interrupt();
        throw e;
      }

      log.info("The task was asked to stop before completing");
    }

    finally

    {
      if (chatHandlerProvider.isPresent()) {
        chatHandlerProvider.get().unregister(getId());
      }
    }

    toolbox.getDataSegmentServerAnnouncer().unannounce();

    //TODO::implement
    return

        success();

  }

  @Override
  public boolean canRestore()
  {
    return true;
  }

  @POST
  @Path("/stop")
  @Override
  public void stopGracefully()
  {
    log.info("Stopping gracefully (status: [%s])", status);
    stopRequested = true;

    synchronized (statusLock) {
      if (status == Status.PUBLISHING) {
        runThread.interrupt();
        return;
      }
    }

    try {
      if (pauseLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          if (pauseRequested) {
            pauseRequested = false;
            shouldResume.signalAll();
          }
        }
        finally {
          pauseLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pauseLock before timeout, interrupting run thread");
        runThread.interrupt();
        return;
      }

      if (pollRetryLock.tryLock(LOCK_ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        try {
          isAwaitingRetry.signalAll();
        }
        finally {
          pollRetryLock.unlock();
        }
      } else {
        log.warn("While stopping: failed to acquire pollRetryLock before timeout, interrupting run thread");
        runThread.interrupt();
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    if (appenderator == null) {
      // Not yet initialized, no data yet, just return a noop runner.
      return new NoopQueryRunner<>();
    }

    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
      {
        return queryPlus.run(appenderator, responseContext);
      }
    };
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Status getStatus()
  {
    return status;
  }

  @GET
  @Path("/offsets/current")
  @Produces(MediaType.APPLICATION_JSON)
  public Integer getCurrentOffsets()
  {
    return nextOffsets;
  }

  @GET
  @Path("/offsets/end")
  @Produces(MediaType.APPLICATION_JSON)
  public Integer getEndOffsets()
  {
    return endOffsets;
  }

  @POST
  @Path("/offsets/end")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response setEndOffsets(
      AtomicInteger offsets,
      @QueryParam("resume")
      @DefaultValue("false")
      final boolean resume
  ) throws InterruptedException
  {
    if (offsets == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity("Request body must contain a map of { partition:endOffset }")
                     .build();
    } else if (endOffsets != offsets.get()) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         String.format(
                             "Request contains partitions not being handled by this task, my partitions: %s",
                             endOffsets
                         )
                     )
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      if (!isPaused()) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity("Task must be paused before changing the end offsets")
                       .build();
      }

      endOffsets = offsets.get();
      log.info("endOffsets changed to %s", endOffsets);
    }
    finally {
      pauseLock.unlock();
    }

    if (resume) {
      resume();
    }

    return Response.ok(endOffsets).build();
  }

  /**
   * Signals the ingestion loop to pause.
   *
   * @param timeout how long to pause for before resuming in milliseconds, <= 0 means indefinitely
   *
   * @return one of the following Responses: 400 Bad Request if the task has started publishing; 202 Accepted if the
   * method has timed out and returned before the task has paused; 200 OK with a map of the current partition offsets
   * in the response body if the task successfully paused
   */
  @POST
  @Path("/pause")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pause(
      @QueryParam("timeout")
      @DefaultValue("0")
      final long timeout
  )
      throws InterruptedException
  {
    if (!(status == Status.PAUSED || status == Status.READING)) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(String.format("Can't pause, task is not in a pausable state (state: [%s])", status))
                     .build();
    }

    pauseLock.lockInterruptibly();
    try {
      pauseMillis = timeout <= 0 ? PAUSE_FOREVER : timeout;
      pauseRequested = true;

      pollRetryLock.lockInterruptibly();
      try {
        isAwaitingRetry.signalAll();
      }
      finally {
        pollRetryLock.unlock();
      }

      if (isPaused()) {
        shouldResume.signalAll(); // kick the monitor so it re-awaits with the new pauseMillis
      }

      long nanos = TimeUnit.SECONDS.toNanos(2);
      while (!isPaused()) {
        if (nanos <= 0L) {
          return Response.status(Response.Status.ACCEPTED)
                         .entity("Request accepted but task has not yet paused")
                         .build();
        }
        nanos = hasPaused.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }

    try {
      return Response.ok().entity(mapper.writeValueAsString(getCurrentOffsets())).build();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @POST
  @Path("/resume")
  public void resume() throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      pauseRequested = false;
      shouldResume.signalAll();

      long nanos = TimeUnit.SECONDS.toNanos(5);
      while (isPaused()) {
        if (nanos <= 0L) {
          throw new RuntimeException("Resume command was not accepted within 5 seconds");
        }
        nanos = shouldResume.awaitNanos(nanos);
      }
    }
    finally {
      pauseLock.unlock();
    }
  }

  @GET
  @Path("/time/start")
  @Produces(MediaType.APPLICATION_JSON)
  public DateTime getStartTime()
  {
    return startTime;
  }

  @VisibleForTesting
  FireDepartmentMetrics getFireDepartmentMetrics()
  {
    return fireDepartmentMetrics;
  }

  private boolean isPaused()
  {
    return status == Status.PAUSED;
  }

  private Appenderator newAppenderator(FireDepartmentMetrics metrics, TaskToolbox toolbox)
  {
    final int maxRowsInMemoryPerPartition = (tuningConfig.getMaxRowsInMemory());
    return Appenderators.createRealtime(
        dataSchema,
        tuningConfig.withBasePersistDirectory(toolbox.getPersistDir())
                    .withMaxRowsInMemory(maxRowsInMemoryPerPartition),
        metrics,
        toolbox.getSegmentPusher(),
        toolbox.getObjectMapper(),
        toolbox.getIndexIO(),
        tuningConfig.getBuildV9Directly() ? toolbox.getIndexMergerV9() : toolbox.getIndexMerger(),
        toolbox.getQueryRunnerFactoryConglomerate(),
        toolbox.getSegmentAnnouncer(),
        toolbox.getEmitter(),
        toolbox.getQueryExecutorService(),
        toolbox.getCache(),
        toolbox.getCacheConfig()
    );
  }

  private AppenderatorDriver newDriver(
      final Appenderator appenderator,
      final TaskToolbox toolbox,
      final FireDepartmentMetrics metrics
  )
  {
    return new AppenderatorDriver(
        appenderator,
        new ActionBasedSegmentAllocator(toolbox.getTaskActionClient(), dataSchema),
        toolbox.getSegmentHandoffNotifierFactory(),
        new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient()),
        toolbox.getObjectMapper(),
        metrics
    );
  }

  /**
   * Checks if the pauseRequested flag was set and if so blocks:
   * a) if pauseMillis == PAUSE_FOREVER, until pauseRequested is cleared
   * b) if pauseMillis != PAUSE_FOREVER, until pauseMillis elapses -or- pauseRequested is cleared
   * <p>
   * If pauseMillis is changed while paused, the new pause timeout will be applied. This allows adjustment of the
   * pause timeout (making a timed pause into an indefinite pause and vice versa is valid) without having to resume
   * and ensures that the loop continues to stay paused without ingesting any new events. You will need to signal
   * shouldResume after adjusting pauseMillis for the new value to take effect.
   * <p>
   * Sets paused = true and signals paused so callers can be notified when the pause command has been accepted.
   * <p>
   * Additionally, pauses if all partitions assignments have been read and pauseAfterRead flag is set.
   *
   * @return true if a pause request was handled, false otherwise
   */
  private boolean possiblyPause(Set<Integer> assignment) throws InterruptedException
  {
    pauseLock.lockInterruptibly();
    try {
      if (ioConfig.isPauseAfterRead() && assignment.isEmpty()) {
        pauseMillis = PAUSE_FOREVER;
        pauseRequested = true;
      }

      if (pauseRequested) {
        status = Status.PAUSED;
        long nanos = 0;
        hasPaused.signalAll();

        while (pauseRequested) {
          if (pauseMillis == PAUSE_FOREVER) {
            log.info("Pausing ingestion until resumed");
            shouldResume.await();
          } else {
            if (pauseMillis > 0) {
              log.info("Pausing ingestion for [%,d] ms", pauseMillis);
              nanos = TimeUnit.MILLISECONDS.toNanos(pauseMillis);
              pauseMillis = 0;
            }
            if (nanos <= 0L) {
              pauseRequested = false; // timeout elapsed
            }
            nanos = shouldResume.awaitNanos(nanos);
          }
        }

        status = Status.READING;
        shouldResume.signalAll();
        log.info("Ingestion loop resumed");
        return true;
      }
    }
    finally {
      pauseLock.unlock();
    }

    return false;
  }

  private String makeQuery(List<String> requiredFields, JDBCPartitions partition)
  {
    if (requiredFields == null) {
      return new StringBuilder("SELECT *  FROM ").append(ioConfig.getTableName()).toString();
    }
    return new StringBuilder("SELECT ").append(StringUtils.join(requiredFields, ','))
                                       .append(" from ")
                                       .append(ioConfig.getTableName())
                                       .append(" where ")
                                       .append(" id >="+partition.getStartOffset() +" and id<="+partition.getEndOffset())
                                       .toString();
  }

  private void verifyParserSpec(ParseSpec parseSpec, List<String> storedColumns) throws IllegalArgumentException
  {
    String tsColumn = parseSpec.getTimestampSpec().getTimestampColumn();
    Preconditions.checkArgument(
        storedColumns.contains(tsColumn),
        String.format("timestamp column %s does not exist in table %s", tsColumn, ioConfig.getTableName())
    );

    for (DimensionSchema dim : parseSpec.getDimensionsSpec().getDimensions()) {
      Preconditions.checkArgument(
          storedColumns.contains(dim.getName()),
          String.format("dimension column %s does not exist in table %s", dim, ioConfig.getTableName())
      );
    }
  }

  public enum Status
  {
    NOT_STARTED,
    STARTING,
    READING,
    PAUSED,
    PUBLISHING
  }
}
