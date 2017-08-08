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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.indexing.IOConfig;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JDBCIOConfigTest {
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private final ObjectMapper mapper;

    public JDBCIOConfigTest() {
        mapper = new DefaultObjectMapper();
        mapper.registerModules((Iterable<Module>) new JDBCIndexTaskModule().getJacksonModules());
    }

    @Test
    public void testSerdeWithDefaults() throws Exception {
        String jsonStr = "{\n"
                + "  \"type\": \"jdbc\",\n"
                + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                + "  \"tableName\": \"dummy\",\n"
                + "  \"user\": \"dummy\",\n"
                + "  \"password\": \"dummy\",\n"
                + "  \"connectURI\": \"dummy\",\n"
                + "  \"driverClass\": \"dummy\",\n"
                + "  \"partitions\": {\"table\":\"table\",\"offsetMaps\":{\"0\":\"10\"},\"interval\":\"10\" },\n"
                + "  \"useTransaction\": true,\n"
                + "  \"pauseAfterRead\": false,\n"
                + "  \"query\": \"dummy\",\n"
                + "  \"columns\": [\"dummy\"]\n"
                + "}";

        JDBCIOConfig config = (JDBCIOConfig) mapper.readValue(
                mapper.writeValueAsString(
                        mapper.readValue(
                                jsonStr,
                                IOConfig.class
                        )
                ), IOConfig.class
        );

        Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
        Assert.assertEquals("dummy", config.getTableName());
        Assert.assertEquals(true, config.isUseTransaction());
        Assert.assertEquals(false, config.isPauseAfterRead());
        Assert.assertFalse("minimumMessageTime", config.getMinimumMessageTime().isPresent());
    }

    @Test
    public void testSerdeWithNonDefaults() throws Exception {
        String jsonStr = "{\n"
                + "  \"type\": \"jdbc\",\n"
                + "  \"baseSequenceName\": \"my-sequence-name\",\n"
                + "  \"tableName\": \"dummy\",\n"
                + "  \"user\": \"dummy\",\n"
                + "  \"password\": \"dummy\",\n"
                + "  \"connectURI\": \"dummy\",\n"
                + "  \"driverClass\": \"dummy\",\n"
                + "  \"partitions\": {\"table\":\"table\",\"offsetMaps\":{\"0\":\"10\"},\"interval\":\"10\" },\n"
                + "  \"query\": \"dummy\",\n"
                + "  \"useTransaction\": false,\n"
                + "  \"pauseAfterRead\": true,\n"
                + "  \"minimumMessageTime\": \"2016-05-31T12:00Z\",\n"
                + "  \"skipOffsetGaps\": true\n"
                + "}";

        JDBCIOConfig config = (JDBCIOConfig) mapper.readValue(
                mapper.writeValueAsString(
                        mapper.readValue(
                                jsonStr,
                                IOConfig.class
                        )
                ), IOConfig.class
        );

        Assert.assertEquals("my-sequence-name", config.getBaseSequenceName());
        Assert.assertEquals("table", config.getJdbcOffsets().getTable());
        Assert.assertEquals(false, config.isUseTransaction());
        Assert.assertEquals(true, config.isPauseAfterRead());
        Assert.assertEquals(new DateTime("2016-05-31T12:00Z"), config.getMinimumMessageTime().get());
    }

    @Test
    public void testBaseSequenceNameRequired() throws Exception {
        String jsonStr = "{\n"
                + "  \"type\": \"jdbc\",\n"
                + "  \"tableName\": \"dummy\",\n"
                + "  \"user\": \"dummy\",\n"
                + "  \"password\": \"dummy\",\n"
                + "  \"connectURI\": \"dummy\",\n"
                + "  \"driverClass\": \"dummy\",\n"
                + "  \"partitions\": {\"table\":\"table\",\"offsetMaps\":{\"0\":\"10\"},\"interval\":\"10\" },\n"
                + "  \"useTransaction\": true,\n"
                + "  \"pauseAfterRead\": false,\n"
                + "  \"query\": \"dummy\",\n"
                + "  \"columns\": [\"dummy\"]\n"
                + "}";

        exception.expect(JsonMappingException.class);
        exception.expectCause(CoreMatchers.isA(NullPointerException.class));
        exception.expectMessage(CoreMatchers.containsString("baseSequenceName"));
        mapper.readValue(jsonStr, IOConfig.class);
    }
}
