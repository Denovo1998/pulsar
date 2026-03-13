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

import java.io.Serializable;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Configuration for the Chronos inner-topic producer used by Pulsar producers.
 *
 * <p>This configuration only affects the dedicated internal producer that publishes Chronos payloads to the
 * configured inner topic. It does not change the behavior of {@link TypedMessageBuilder#deliverAfter(long,
 * java.util.concurrent.TimeUnit)} or {@link TypedMessageBuilder#deliverAt(long)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ChronosProducerConfiguration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    public static final long DEFAULT_SEND_TIMEOUT_MS = 30000L;
    public static final boolean DEFAULT_BLOCK_IF_QUEUE_FULL = true;
    public static final int DEFAULT_MAX_PENDING_MESSAGES = 1000;

    private String innerTopic;
    private long sendTimeoutMs = DEFAULT_SEND_TIMEOUT_MS;
    private boolean blockIfQueueFull = DEFAULT_BLOCK_IF_QUEUE_FULL;
    private int maxPendingMessages = DEFAULT_MAX_PENDING_MESSAGES;

    public ChronosProducerConfiguration() {
    }

    public ChronosProducerConfiguration(ChronosProducerConfiguration other) {
        this.innerTopic = other.innerTopic;
        this.sendTimeoutMs = other.sendTimeoutMs;
        this.blockIfQueueFull = other.blockIfQueueFull;
        this.maxPendingMessages = other.maxPendingMessages;
    }

    public String getInnerTopic() {
        return innerTopic;
    }

    public ChronosProducerConfiguration setInnerTopic(String innerTopic) {
        if (innerTopic == null || innerTopic.trim().isEmpty()) {
            throw new IllegalArgumentException("innerTopic cannot be blank");
        }
        this.innerTopic = innerTopic.trim();
        return this;
    }

    public long getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    public ChronosProducerConfiguration setSendTimeoutMs(long sendTimeoutMs) {
        if (sendTimeoutMs < 0) {
            throw new IllegalArgumentException("sendTimeoutMs needs to be >= 0");
        }
        this.sendTimeoutMs = sendTimeoutMs;
        return this;
    }

    public boolean isBlockIfQueueFull() {
        return blockIfQueueFull;
    }

    public ChronosProducerConfiguration setBlockIfQueueFull(boolean blockIfQueueFull) {
        this.blockIfQueueFull = blockIfQueueFull;
        return this;
    }

    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    public ChronosProducerConfiguration setMaxPendingMessages(int maxPendingMessages) {
        if (maxPendingMessages < 0) {
            throw new IllegalArgumentException("maxPendingMessages needs to be >= 0");
        }
        this.maxPendingMessages = maxPendingMessages;
        return this;
    }

    @Override
    public ChronosProducerConfiguration clone() {
        return new ChronosProducerConfiguration(this);
    }
}
