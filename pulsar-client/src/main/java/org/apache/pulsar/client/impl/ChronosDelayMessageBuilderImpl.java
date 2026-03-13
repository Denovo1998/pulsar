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
import com.xiaojukeji.carrera.chronos.protocol.ChronosSendResult;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.ChronosDelayMessageBuilder;

class ChronosDelayMessageBuilderImpl<T> implements ChronosDelayMessageBuilder<T> {

    private final ChronosMessageSender<T> sender;
    private final AtomicBoolean sent = new AtomicBoolean(false);
    private final Map<String, String> properties = new LinkedHashMap<>();
    private T value;
    private ChronosDelayMeta delayMeta;
    private String[] tags;

    ChronosDelayMessageBuilderImpl(ChronosMessageSender<T> sender) {
        this.sender = sender;
    }

    @Override
    public ChronosDelayMessageBuilder<T> value(T value) {
        this.value = value;
        return this;
    }

    @Override
    public ChronosDelayMessageBuilder<T> delayMeta(ChronosDelayMeta meta) {
        this.delayMeta = meta == null ? null : new ChronosDelayMeta(meta);
        return this;
    }

    @Override
    public ChronosDelayMessageBuilder<T> tags(String... tags) {
        this.tags = tags == null ? null : Arrays.copyOf(tags, tags.length);
        return this;
    }

    @Override
    public ChronosDelayMessageBuilder<T> property(String key, String value) {
        if (key == null) {
            throw new IllegalArgumentException("Need Non-Null key");
        }
        if (value == null) {
            throw new IllegalArgumentException("Need Non-Null value for key: " + key);
        }
        properties.put(key, value);
        return this;
    }

    @Override
    public ChronosDelayMessageBuilder<T> properties(Map<String, String> properties) {
        if (properties == null) {
            throw new IllegalArgumentException("Need Non-Null properties");
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            property(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @Override
    public ChronosSendResult send() {
        return sendAsync().join();
    }

    @Override
    public CompletableFuture<ChronosSendResult> sendAsync() {
        if (!sent.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(new ChronosSendResult(
                    ChronosSendResult.FAIL_UNKNOWN,
                    "Chronos message builder can only send once",
                    ""));
        }
        return sender.sendAsync(value, delayMeta, tags, new LinkedHashMap<>(properties));
    }
}
