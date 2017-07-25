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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexing.jdbc.JDBCIndexTaskModule;
import io.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JDBCSupervisorIOConfigTest
{
  private final ObjectMapper mapper;

  public JDBCSupervisorIOConfigTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.registerModules((Iterable<Module>) new JDBCIndexTaskModule().getJacksonModules());
  }

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"jdbc\",\n"
                     + "  \"table\": \"my-table\",\n"
                     + "  \"user\": \"dummy\",\n"
                     + "  \"password\": \"dummy\",\n"
                     + "  \"connectURI\": \"dummy\",\n"
                     + "  \"driverClass\": \"dummy\",\n"
                     + "  \"query\": \"dummy\",\n"
                     + "  \"columns\": []\n"
                     + "}";

    JDBCSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                JDBCSupervisorIOConfig.class
            )
        ), JDBCSupervisorIOConfig.class
    );

    Assert.assertEquals("my-table", config.getTable());
    Assert.assertEquals(1, (int) config.getReplicas());
    Assert.assertEquals(1, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(60), config.getTaskDuration());
    Assert.assertEquals(Duration.standardSeconds(5), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(30), config.getPeriod());
    Assert.assertEquals(Duration.standardMinutes(30), config.getCompletionTimeout());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"jdbc\",\n"
                     + "  \"table\": \"my-table\",\n"
                     + "  \"user\": \"dummy\",\n"
                     + "  \"password\": \"dummy\",\n"
                     + "  \"connectURI\": \"dummy\",\n"
                     + "  \"driverClass\": \"dummy\",\n"
                     + "  \"query\": \"dummy\",\n"
                     + "  \"columns\": [],\n"
                     + "  \"replicas\": 3,\n"
                     + "  \"taskCount\": 9,\n"
                     + "  \"taskDuration\": \"PT30M\",\n"
                     + "  \"startDelay\": \"PT1M\",\n"
                     + "  \"period\": \"PT10S\",\n"
                     + "  \"useEarliestOffset\": true,\n"
                     + "  \"completionTimeout\": \"PT45M\",\n"
                     + "  \"lateMessageRejectionPeriod\": \"PT1H\",\n"
                     + "  \"skipOffsetGaps\": true\n"
                     + "}";

    JDBCSupervisorIOConfig config = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                JDBCSupervisorIOConfig.class
            )
        ), JDBCSupervisorIOConfig.class
    );

    Assert.assertEquals("my-table", config.getTable());
    Assert.assertEquals(3, (int) config.getReplicas());
    Assert.assertEquals(9, (int) config.getTaskCount());
    Assert.assertEquals(Duration.standardMinutes(30), config.getTaskDuration());
    Assert.assertEquals(Duration.standardMinutes(1), config.getStartDelay());
    Assert.assertEquals(Duration.standardSeconds(10), config.getPeriod());
    Assert.assertEquals(Duration.standardMinutes(45), config.getCompletionTimeout());
  }

  @Test
  public void testTableRequired() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"jdbc\",\n"
//                     + "  \"table\": \"my-table\",\n"
                     + "  \"user\": \"dummy\",\n"
                     + "  \"password\": \"dummy\",\n"
                     + "  \"connectURI\": \"dummy\",\n"
                     + "  \"driverClass\": \"dummy\",\n"
                     + "  \"query\": \"dummy\",\n"
                     + "  \"columns\": []\n"
                     + "}";

    exception.expect(JsonMappingException.class);
    exception.expectCause(CoreMatchers.isA(NullPointerException.class));
    exception.expectMessage(CoreMatchers.containsString("table"));
    mapper.readValue(jsonStr, JDBCSupervisorIOConfig.class);
  }
}
