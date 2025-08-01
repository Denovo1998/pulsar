/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.rabbitmq.sink;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.rabbitmq.RabbitMQSinkConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * RabbitMQSinkConfig test.
 */
public class RabbitMQSinkConfigTest {
    @Test
    public final void loadFromYamlFileTest() throws IOException {
        File yamlFile = getFile("sinkConfig.yaml");
        String path = yamlFile.getAbsolutePath();
        RabbitMQSinkConfig config = RabbitMQSinkConfig.load(path);
        assertNotNull(config);
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.parseInt("5673"));
        assertEquals(config.getVirtualHost(), "/");
        assertEquals(config.getUsername(), "guest");
        assertEquals(config.getPassword(), "guest");
        assertEquals(config.getConnectionName(), "test-connection");
        assertEquals(config.getRequestedChannelMax(), Integer.parseInt("0"));
        assertEquals(config.getRequestedFrameMax(), Integer.parseInt("0"));
        assertEquals(config.getConnectionTimeout(), Integer.parseInt("60000"));
        assertEquals(config.getHandshakeTimeout(), Integer.parseInt("10000"));
        assertEquals(config.getRequestedHeartbeat(), Integer.parseInt("60"));
        assertEquals(config.getExchangeName(), "test-exchange");
        assertEquals(config.getExchangeType(), "test-exchange-type");
    }

    @Test
    public final void loadFromMapTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5673");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("exchangeName", "test-exchange");
        map.put("exchangeType", "test-exchange-type");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        RabbitMQSinkConfig config = RabbitMQSinkConfig.load(map, sinkContext);
        assertNotNull(config);
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.parseInt("5673"));
        assertEquals(config.getVirtualHost(), "/");
        assertEquals(config.getUsername(), "guest");
        assertEquals(config.getPassword(), "guest");
        assertEquals(config.getConnectionName(), "test-connection");
        assertEquals(config.getRequestedChannelMax(), Integer.parseInt("0"));
        assertEquals(config.getRequestedFrameMax(), Integer.parseInt("0"));
        assertEquals(config.getConnectionTimeout(), Integer.parseInt("60000"));
        assertEquals(config.getHandshakeTimeout(), Integer.parseInt("10000"));
        assertEquals(config.getRequestedHeartbeat(), Integer.parseInt("60"));
        assertEquals(config.getExchangeName(), "test-exchange");
        assertEquals(config.getExchangeType(), "test-exchange-type");
    }

    @Test
    public final void loadFromMapCredentialsFromSecretTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5673");
        map.put("virtualHost", "/");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("exchangeName", "test-exchange");
        map.put("exchangeType", "test-exchange-type");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        Mockito.when(sinkContext.getSecret("username"))
                .thenReturn("guest");
        Mockito.when(sinkContext.getSecret("password"))
                .thenReturn("guest");
        RabbitMQSinkConfig config = RabbitMQSinkConfig.load(map, sinkContext);
        assertNotNull(config);
        assertEquals(config.getHost(), "localhost");
        assertEquals(config.getPort(), Integer.parseInt("5673"));
        assertEquals(config.getVirtualHost(), "/");
        assertEquals(config.getUsername(), "guest");
        assertEquals(config.getPassword(), "guest");
        assertEquals(config.getConnectionName(), "test-connection");
        assertEquals(config.getRequestedChannelMax(), Integer.parseInt("0"));
        assertEquals(config.getRequestedFrameMax(), Integer.parseInt("0"));
        assertEquals(config.getConnectionTimeout(), Integer.parseInt("60000"));
        assertEquals(config.getHandshakeTimeout(), Integer.parseInt("10000"));
        assertEquals(config.getRequestedHeartbeat(), Integer.parseInt("60"));
        assertEquals(config.getExchangeName(), "test-exchange");
        assertEquals(config.getExchangeType(), "test-exchange-type");
    }

    @Test
    public final void validValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5673");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("exchangeName", "test-exchange");
        map.put("exchangeType", "test-exchange-type");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        RabbitMQSinkConfig config = RabbitMQSinkConfig.load(map, sinkContext);
        config.validate();
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
        expectedExceptionsMessageRegExp = "exchangeName cannot be null")
    public final void missingExchangeValidateTest() throws IOException {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "localhost");
        map.put("port", "5673");
        map.put("virtualHost", "/");
        map.put("username", "guest");
        map.put("password", "guest");
        map.put("connectionName", "test-connection");
        map.put("requestedChannelMax", "0");
        map.put("requestedFrameMax", "0");
        map.put("connectionTimeout", "60000");
        map.put("handshakeTimeout", "10000");
        map.put("requestedHeartbeat", "60");
        map.put("exchangeType", "test-exchange-type");

        SinkContext sinkContext = Mockito.mock(SinkContext.class);
        RabbitMQSinkConfig config = RabbitMQSinkConfig.load(map, sinkContext);
        config.validate();
    }

    private File getFile(String name) {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(name).getFile());
    }
}
