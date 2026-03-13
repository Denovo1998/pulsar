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
package org.apache.pulsar.client.api;

import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayMeta;
import com.xiaojukeji.carrera.chronos.protocol.ChronosSendResult;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Builder for Chronos messages sent through a Pulsar producer.
 *
 * <p>This builder always publishes a Chronos-encoded payload to the configured inner topic. It does not reuse
 * Pulsar delayed delivery or message metadata based routing.
 *
 * <p>Instances are single-use. A builder should be configured and sent once.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ChronosDelayMessageBuilder<T> {

    /**
     * Set the business payload to encode with the producer schema.
     */
    ChronosDelayMessageBuilder<T> value(T value);

    /**
     * Set the Chronos delay metadata.
     */
    ChronosDelayMessageBuilder<T> delayMeta(ChronosDelayMeta meta);

    /**
     * Set Chronos tags for the current send request.
     */
    ChronosDelayMessageBuilder<T> tags(String... tags);

    /**
     * Add a Chronos property for the current send request.
     */
    ChronosDelayMessageBuilder<T> property(String key, String value);

    /**
     * Add Chronos properties for the current send request.
     */
    ChronosDelayMessageBuilder<T> properties(Map<String, String> properties);

    /**
     * Encode and publish the Chronos request synchronously.
     */
    ChronosSendResult send();

    /**
     * Encode and publish the Chronos request asynchronously.
     */
    CompletableFuture<ChronosSendResult> sendAsync();
}
