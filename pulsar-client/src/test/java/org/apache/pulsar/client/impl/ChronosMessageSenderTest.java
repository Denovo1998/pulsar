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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xiaojukeji.carrera.chronos.protocol.ChronosAction;
import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayMeta;
import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayType;
import com.xiaojukeji.carrera.chronos.protocol.ChronosSendResult;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.ChronosProducerConfiguration;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class ChronosMessageSenderTest {

    @Test
    public void testSendAsyncReturnsDisabledResultWhenChronosIsNotEnabled() {
        ChronosMessageSender<String> sender = new ChronosMessageSender<>(
                mock(PulsarClientImpl.class),
                "persistent://public/default/business-topic",
                Schema.STRING,
                null);

        ChronosSendResult result = sender.sendAsync("hello", newDelayMeta(), null, null).join();
        assertEquals(result.getCode(), ChronosSendResult.FAIL_UNKNOWN);
        assertEquals(result.getMsg(), ChronosMessageSender.CHRONOS_NOT_ENABLED_MESSAGE);
    }

    @Test
    public void testSendAsyncEncodesAndPublishesToInnerTopic() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Producer<byte[]> innerProducer = mock(Producer.class);
        TypedMessageBuilder<byte[]> innerBuilder = mock(TypedMessageBuilder.class);
        when(innerProducer.newMessage()).thenReturn(innerBuilder);
        when(innerBuilder.key(any())).thenReturn(innerBuilder);
        when(innerBuilder.value(any())).thenReturn(innerBuilder);
        when(innerBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.earliest));

        ArgumentCaptor<ProducerConfigurationData> confCaptor = ArgumentCaptor.forClass(ProducerConfigurationData.class);
        when(client.createProducerAsync(confCaptor.capture(), eq(Schema.BYTES), eq(null)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(innerProducer));

        ChronosMessageSender<String> sender = new ChronosMessageSender<>(client,
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        ChronosSendResult result = sender.sendAsync("hello chronos", newDelayMeta(),
                new String[]{"tagA", "tagB"},
                Map.of("traceId", "trace-1")).join();

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(innerBuilder).key(keyCaptor.capture());
        verify(innerBuilder).value(payloadCaptor.capture());
        verify(innerBuilder).sendAsync();

        ProducerConfigurationData innerConf = confCaptor.getValue();
        assertEquals(innerConf.getTopicName(), "persistent://public/default/chronos-inner");
        assertEquals(innerConf.getSendTimeoutMs(), 30000L);
        assertTrue(innerConf.isBlockIfQueueFull());
        assertEquals(innerConf.getMaxPendingMessages(), 1000);

        JSONObject payload = JSON.parseObject(new String(payloadCaptor.getValue(), StandardCharsets.UTF_8));
        assertEquals(result.getCode(), ChronosSendResult.OK);
        assertEquals(keyCaptor.getValue(), result.getUniqDelayMsgId());
        assertEquals(payload.getString("topic"), "persistent://public/default/business-topic");
        assertEquals(payload.getInteger("action").intValue(), ChronosAction.ADD.getCode());
        assertEquals(payload.getString("body"), "hello chronos");
        assertEquals(payload.getString("tags"), "tagA||tagB");
        assertEquals(payload.getJSONObject("properties").getString("traceId"), "trace-1");
    }

    @Test
    public void testSendAsyncRejectsMissingValueOrDelayMeta() {
        ChronosMessageSender<String> sender = new ChronosMessageSender<>(mock(PulsarClientImpl.class),
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        ChronosSendResult missingValue = sender.sendAsync(null, newDelayMeta(), null, null).join();
        ChronosSendResult missingDelayMeta = sender.sendAsync("hello", null, null, null).join();

        assertEquals(missingValue.getCode(), ChronosSendResult.FAIL_ILLEGAL_MSG);
        assertEquals(missingDelayMeta.getCode(), ChronosSendResult.FAIL_ILLEGAL_MSG);
    }

    @Test
    public void testSendAsyncRejectsEmptyEncodedBody() {
        ChronosMessageSender<byte[]> sender = new ChronosMessageSender<>(mock(PulsarClientImpl.class),
                "persistent://public/default/business-topic",
                Schema.BYTES,
                enabledConfiguration());

        ChronosSendResult result = sender.sendAsync(new byte[0], newDelayMeta(), null, null).join();

        assertEquals(result.getCode(), ChronosSendResult.FAIL_ILLEGAL_MSG);
        assertEquals(result.getMsg(), "Chronos message body is required and cannot be empty");
    }

    @Test
    public void testCancelAsyncEncodesCancelPayload() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Producer<byte[]> innerProducer = mock(Producer.class);
        TypedMessageBuilder<byte[]> innerBuilder = mock(TypedMessageBuilder.class);
        when(innerProducer.newMessage()).thenReturn(innerBuilder);
        when(innerBuilder.key(any())).thenReturn(innerBuilder);
        when(innerBuilder.value(any())).thenReturn(innerBuilder);
        when(innerBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.earliest));
        when(client.createProducerAsync(any(ProducerConfigurationData.class), eq(Schema.BYTES), eq(null)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(innerProducer));

        ChronosMessageSender<String> sender = new ChronosMessageSender<>(client,
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        String uniqDelayMsgId = "1760000000-2-1760172800-0-0-0-0-123e4567-e89b-12d3-a456-426614174000";
        ChronosSendResult result = sender.cancelAsync(uniqDelayMsgId, "tag-cancel").join();

        ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(innerBuilder).value(payloadCaptor.capture());
        JSONObject payload = JSON.parseObject(new String(payloadCaptor.getValue(), StandardCharsets.UTF_8));
        assertEquals(result.getCode(), ChronosSendResult.OK);
        assertEquals(payload.getInteger("action").intValue(), ChronosAction.CANCEL.getCode());
        assertEquals(payload.getString("body"), "c");
        assertEquals(payload.getString("tags"), "tag-cancel");
    }

    @Test
    public void testSendAsyncRejectsSeparatedKeyValueSchema() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> separatedSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.SEPARATED);

        ChronosMessageSender<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> sender =
                new ChronosMessageSender<>(mock(PulsarClientImpl.class),
                        "persistent://public/default/business-topic",
                        separatedSchema,
                        enabledConfiguration());

        ChronosSendResult result = sender.sendAsync(new KeyValue<>(new SchemaTestUtils.Foo(), new SchemaTestUtils.Bar()),
                newDelayMeta(), null, null).join();

        assertEquals(result.getCode(), ChronosSendResult.FAIL_ILLEGAL_MSG);
    }

    @Test
    public void testCloseAsyncClosesInnerProducer() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Producer<byte[]> innerProducer = mock(Producer.class);
        TypedMessageBuilder<byte[]> innerBuilder = mock(TypedMessageBuilder.class);
        when(innerProducer.newMessage()).thenReturn(innerBuilder);
        when(innerBuilder.key(any())).thenReturn(innerBuilder);
        when(innerBuilder.value(any())).thenReturn(innerBuilder);
        when(innerBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.earliest));
        when(innerProducer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
        when(client.createProducerAsync(any(ProducerConfigurationData.class), eq(Schema.BYTES), eq(null)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(innerProducer));

        ChronosMessageSender<String> sender = new ChronosMessageSender<>(client,
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        sender.sendAsync("hello chronos", newDelayMeta(), null, null).join();
        sender.closeAsync().join();

        verify(innerProducer).closeAsync();
    }

    @Test
    public void testSendAsyncCreatesInnerProducerOnce() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Producer<byte[]> innerProducer = mock(Producer.class);
        TypedMessageBuilder<byte[]> innerBuilder = mock(TypedMessageBuilder.class);
        when(innerProducer.newMessage()).thenReturn(innerBuilder);
        when(innerBuilder.key(any())).thenReturn(innerBuilder);
        when(innerBuilder.value(any())).thenReturn(innerBuilder);
        when(innerBuilder.sendAsync()).thenReturn(CompletableFuture.completedFuture(MessageId.earliest));
        when(client.createProducerAsync(any(ProducerConfigurationData.class), eq(Schema.BYTES), eq(null)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(innerProducer));

        ChronosMessageSender<String> sender = new ChronosMessageSender<>(client,
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        sender.sendAsync("first", newDelayMeta(), null, null).join();
        sender.sendAsync("second", newDelayMeta(), null, null).join();

        verify(client).createProducerAsync(any(ProducerConfigurationData.class), eq(Schema.BYTES), eq(null));
    }

    @Test
    public void testSendAsyncMapsTimeoutToFailTimeout() {
        PulsarClientImpl client = mock(PulsarClientImpl.class);
        Producer<byte[]> innerProducer = mock(Producer.class);
        TypedMessageBuilder<byte[]> innerBuilder = mock(TypedMessageBuilder.class);
        when(innerProducer.newMessage()).thenReturn(innerBuilder);
        when(innerBuilder.key(any())).thenReturn(innerBuilder);
        when(innerBuilder.value(any())).thenReturn(innerBuilder);
        when(innerBuilder.sendAsync()).thenReturn(
                CompletableFuture.failedFuture(new PulsarClientException.TimeoutException("chronos timeout")));
        when(client.createProducerAsync(any(ProducerConfigurationData.class), eq(Schema.BYTES), eq(null)))
                .thenReturn((CompletableFuture) CompletableFuture.completedFuture(innerProducer));

        ChronosMessageSender<String> sender = new ChronosMessageSender<>(client,
                "persistent://public/default/business-topic",
                Schema.STRING,
                enabledConfiguration());

        ChronosSendResult result = sender.sendAsync("hello", newDelayMeta(), null, null).join();

        assertEquals(result.getCode(), ChronosSendResult.FAIL_TIMEOUT);
        assertEquals(result.getMsg(), "chronos timeout");
    }

    private ChronosProducerConfiguration enabledConfiguration() {
        return new ChronosProducerConfiguration().setInnerTopic("persistent://public/default/chronos-inner");
    }

    private ChronosDelayMeta newDelayMeta() {
        return new ChronosDelayMeta()
                .setTimestamp(System.currentTimeMillis() / 1000 + 60)
                .setDelayType(ChronosDelayType.DELAY)
                .setTimes(0)
                .setInterval(0);
    }
}
