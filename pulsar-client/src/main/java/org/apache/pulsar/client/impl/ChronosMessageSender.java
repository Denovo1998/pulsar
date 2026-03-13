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

import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayMeta;
import com.xiaojukeji.carrera.chronos.protocol.ChronosMessageCodec;
import com.xiaojukeji.carrera.chronos.protocol.ChronosRequestValidator;
import com.xiaojukeji.carrera.chronos.protocol.ChronosSendResult;
import com.xiaojukeji.carrera.chronos.protocol.EncodedChronosMessage;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.pulsar.client.api.ChronosProducerConfiguration;
import org.apache.pulsar.client.api.EncodeData;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.common.schema.KeyValueEncodingType;

class ChronosMessageSender<T> {

    static final String CHRONOS_NOT_ENABLED_MESSAGE = "Chronos is not enabled for this producer";
    private static final String OK_MESSAGE = "OK";
    private static final String CHRONOS_ILLEGAL_MESSAGE = "Chronos message is illegal";

    private final PulsarClientImpl client;
    private final String logicalTopic;
    private final Schema<T> schema;
    private final ChronosProducerConfiguration chronosConfiguration;
    private final AtomicReference<CompletableFuture<Producer<byte[]>>> innerProducerFuture = new AtomicReference<>();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    ChronosMessageSender(PulsarClientImpl client, String logicalTopic, Schema<T> schema,
                         ChronosProducerConfiguration chronosConfiguration) {
        this.client = client;
        this.logicalTopic = logicalTopic;
        this.schema = schema;
        this.chronosConfiguration = chronosConfiguration == null ? null : chronosConfiguration.clone();
    }

    CompletableFuture<ChronosSendResult> sendAsync(T value, ChronosDelayMeta delayMeta, String[] tags,
                                                   Map<String, String> properties) {
        if (!isEnabled()) {
            return CompletableFuture.completedFuture(notEnabledResult());
        }
        if (value == null || delayMeta == null) {
            return CompletableFuture.completedFuture(illegalMessageResult("",
                    "Chronos value and delayMeta are required"));
        }
        if (isSeparatedKeyValueSchema()) {
            return CompletableFuture.completedFuture(illegalMessageResult("",
                    "Chronos does not support schemas that require Pulsar message metadata"));
        }

        final byte[] body;
        try {
            EncodeData encodeData = schema.encode(logicalTopic, value);
            if (encodeData == null || encodeData.data() == null || encodeData.data().length == 0) {
                return CompletableFuture.completedFuture(illegalMessageResult("",
                        "Chronos message body is required and cannot be empty"));
            }
            body = encodeData.data();
        } catch (SchemaSerializationException | UnsupportedOperationException | IllegalArgumentException e) {
            return CompletableFuture.completedFuture(illegalMessageResult("", e.getMessage()));
        }

        ChronosDelayMeta copiedDelayMeta = new ChronosDelayMeta(delayMeta);
        ChronosSendResult validation = ChronosRequestValidator.validateAdd(logicalTopic, body, copiedDelayMeta);
        if (validation != null) {
            return CompletableFuture.completedFuture(validation);
        }

        EncodedChronosMessage encodedMessage = ChronosMessageCodec.encodeAdd(logicalTopic, body, copiedDelayMeta,
                normalizeProperties(properties), tags);
        return sendEncodedAsync(encodedMessage);
    }

    CompletableFuture<ChronosSendResult> cancelAsync(String uniqDelayMsgId, String... tags) {
        if (!isEnabled()) {
            return CompletableFuture.completedFuture(notEnabledResult());
        }

        ChronosSendResult validation = ChronosRequestValidator.validateCancel(logicalTopic, uniqDelayMsgId);
        if (validation != null) {
            return CompletableFuture.completedFuture(validation);
        }

        EncodedChronosMessage encodedMessage = ChronosMessageCodec.encodeCancel(logicalTopic, uniqDelayMsgId, tags);
        return sendEncodedAsync(encodedMessage);
    }

    CompletableFuture<Void> closeAsync() {
        closing.set(true);
        CompletableFuture<Producer<byte[]>> future = innerProducerFuture.get();
        if (future == null) {
            return CompletableFuture.completedFuture(null);
        }
        return future.handle((producer, error) -> {
            if (error != null || producer == null) {
                return CompletableFuture.<Void>completedFuture(null);
            }
            return producer.closeAsync();
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<ChronosSendResult> sendEncodedAsync(EncodedChronosMessage encodedMessage) {
        return getOrCreateInnerProducer()
                .thenCompose(producer -> producer.newMessage()
                        .key(encodedMessage.getKey())
                        .value(encodedMessage.getPayload())
                        .sendAsync()
                        .thenApply(__ -> successResult(encodedMessage.getUniqDelayMsgId())))
                .exceptionally(error -> mapFailure(error, encodedMessage.getUniqDelayMsgId()));
    }

    private CompletableFuture<Producer<byte[]>> getOrCreateInnerProducer() {
        if (closing.get()) {
            return CompletableFuture.failedFuture(
                    new PulsarClientException.AlreadyClosedException("Chronos producer already closed"));
        }

        CompletableFuture<Producer<byte[]>> existing = innerProducerFuture.get();
        if (existing != null) {
            return existing;
        }

        CompletableFuture<Producer<byte[]>> placeholder = new CompletableFuture<>();
        if (!innerProducerFuture.compareAndSet(null, placeholder)) {
            return innerProducerFuture.get();
        }

        ProducerConfigurationData innerConf = new ProducerConfigurationData();
        innerConf.setTopicName(chronosConfiguration.getInnerTopic());
        innerConf.setSendTimeoutMs(chronosConfiguration.getSendTimeoutMs());
        innerConf.setBlockIfQueueFull(chronosConfiguration.isBlockIfQueueFull());
        innerConf.setMaxPendingMessages(chronosConfiguration.getMaxPendingMessages());

        client.createProducerAsync(innerConf, Schema.BYTES, null).whenComplete((producer, error) -> {
            if (error != null) {
                innerProducerFuture.compareAndSet(placeholder, null);
                placeholder.completeExceptionally(error);
            } else {
                placeholder.complete(producer);
            }
        });
        return placeholder;
    }

    private boolean isEnabled() {
        return chronosConfiguration != null;
    }

    private boolean isSeparatedKeyValueSchema() {
        return schema instanceof KeyValueSchema
                && ((KeyValueSchema<?, ?>) schema).getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED;
    }

    private ChronosSendResult successResult(String uniqDelayMsgId) {
        return new ChronosSendResult(ChronosSendResult.OK, OK_MESSAGE, uniqDelayMsgId);
    }

    private ChronosSendResult notEnabledResult() {
        return new ChronosSendResult(ChronosSendResult.FAIL_UNKNOWN, CHRONOS_NOT_ENABLED_MESSAGE, "");
    }

    private ChronosSendResult illegalMessageResult(String uniqDelayMsgId, String message) {
        return new ChronosSendResult(ChronosSendResult.FAIL_ILLEGAL_MSG,
                message == null || message.isEmpty() ? CHRONOS_ILLEGAL_MESSAGE : message,
                uniqDelayMsgId);
    }

    private ChronosSendResult mapFailure(Throwable error, String uniqDelayMsgId) {
        Throwable cause = unwrap(error);
        if (cause instanceof PulsarClientException.TimeoutException) {
            return new ChronosSendResult(ChronosSendResult.FAIL_TIMEOUT,
                    cause.getMessage() == null ? "FAILED: Send timeout" : cause.getMessage(),
                    uniqDelayMsgId);
        }
        return new ChronosSendResult(ChronosSendResult.FAIL_UNKNOWN,
                cause == null || cause.getMessage() == null ? "Chronos send failed" : cause.getMessage(),
                uniqDelayMsgId);
    }

    private Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return unwrap(error.getCause());
        }
        return error;
    }

    private Map<String, String> normalizeProperties(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return null;
        }
        return new LinkedHashMap<>(properties);
    }
}
