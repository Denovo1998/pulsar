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
package org.apache.pulsar.broker.service.trace;

import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.apache.pulsar.common.naming.TopicName;

@Getter
public class TopicTraceOperation {
    private final TopicTraceManager manager;
    private final TopicName topicName;
    private final TopicTraceEventType eventType;
    private final TopicTraceContext context;
    private final String operationId;
    private String subscription;
    private String producerName;
    private Long producerId;
    private String consumerName;
    private Long consumerId;
    private Long ledgerId;
    private String remoteCluster;

    TopicTraceOperation(TopicTraceManager manager, TopicName topicName, TopicTraceEventType eventType,
                        TopicTraceContext context, String operationId) {
        this.manager = manager;
        this.topicName = topicName;
        this.eventType = eventType;
        this.context = context;
        this.operationId = operationId;
    }

    public TopicTraceOperation subscription(String subscription) {
        this.subscription = subscription;
        return this;
    }

    public TopicTraceOperation producer(String producerName, Long producerId) {
        this.producerName = producerName;
        this.producerId = producerId;
        return this;
    }

    public TopicTraceOperation consumer(String consumerName, Long consumerId) {
        this.consumerName = consumerName;
        this.consumerId = consumerId;
        return this;
    }

    public TopicTraceOperation ledger(Long ledgerId) {
        this.ledgerId = ledgerId;
        return this;
    }

    public TopicTraceOperation remoteCluster(String remoteCluster) {
        this.remoteCluster = remoteCluster;
        return this;
    }

    public CompletableFuture<Void> before(String summary, Object detail) {
        return manager.emit(this, TopicTraceStage.BEFORE, summary, null, null, detail, null);
    }

    public CompletableFuture<Void> success(String summary, Object before, Object after, Object detail) {
        return manager.emit(this, TopicTraceStage.SUCCESS, summary, before, after, detail, null);
    }

    public CompletableFuture<Void> success(String summary, Object detail) {
        return success(summary, null, null, detail);
    }

    public CompletableFuture<Void> failure(Throwable error, String summary, Object before, Object after, Object detail) {
        return manager.emit(this, TopicTraceStage.FAILURE, summary, before, after, detail, error);
    }

    public CompletableFuture<Void> failure(Throwable error, String summary, Object detail) {
        return failure(error, summary, null, null, detail);
    }
}
