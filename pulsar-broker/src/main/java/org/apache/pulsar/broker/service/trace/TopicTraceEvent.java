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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicTraceEvent {
    private String eventId;
    private String operationId;
    private long eventTime;
    private String cluster;
    private String broker;
    private String topic;
    private String partitionedTopic;
    private String tenant;
    private String namespace;
    private Integer partitionIndex;
    private TopicTraceEventType eventType;
    private TopicTraceStage stage;
    private TopicTraceSourceType sourceType;
    private String principal;
    private String originalPrincipal;
    private String clientAddress;
    private Long requestId;
    private String requestPath;
    private String requestMethod;
    private String listenerName;
    private String subscription;
    private String producerName;
    private Long producerId;
    private String consumerName;
    private Long consumerId;
    private Long ledgerId;
    private String remoteCluster;
    private String summary;
    private String beforeJson;
    private String afterJson;
    private String detailJson;
    private String errorClass;
    private String errorMessage;
}
