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

import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;

@Slf4j
public class TopicTraceManager implements TopicEventsListener, AutoCloseable {
    public static final String TOPIC_TRACE_ENABLED_PROPERTY = "pulsar.topic-trace.enabled";
    public static final String TOPIC_TRACE_EVENT_TYPES_PROPERTY = "pulsar.topic-trace.event-types";

    private static final Schema<TopicTraceEvent> SCHEMA = Schema.AVRO(TopicTraceEvent.class);

    private final PulsarService pulsar;
    private final ObjectWriter objectWriter = ObjectMapperFactory.getMapper().writer();
    private final ConcurrentHashMap<NamespaceName, CompletableFuture<Producer<TopicTraceEvent>>> writers =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<LifecycleOperationKey, String> lifecycleOperations = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TopicTraceManager(PulsarService pulsar) {
        this.pulsar = pulsar;
    }

    public TopicTraceOperation newOperation(TopicName topicName, TopicTraceEventType eventType,
                                            TopicTraceContext context) {
        return new TopicTraceOperation(this, topicName, eventType,
                context == null ? TopicTraceContext.internal() : context, newOperationId());
    }

    public CompletableFuture<Void> emitTerminal(TopicName topicName, TopicTraceEventType eventType,
                                                TopicTraceContext context, String summary, Object detail) {
        return emit(newOperation(topicName, eventType, context), TopicTraceStage.SUCCESS,
                summary, null, null, detail, null);
    }

    CompletableFuture<Void> emit(TopicTraceOperation operation, TopicTraceStage stage, String summary,
                                 Object before, Object after, Object detail, Throwable error) {
        TopicName topicName = TopicName.get(operation.getTopicName().toString());
        if (closed.get() || SystemTopicNames.isSystemTopic(topicName)) {
            return CompletableFuture.completedFuture(null);
        }
        return resolveSettingsAsync(topicName)
                .thenCompose(settings -> {
                    if (!settings.allows(operation.getEventType())) {
                        return CompletableFuture.<Void>completedFuture(null);
                    }
                    TopicTraceEvent event = buildEvent(operation, topicName, stage, summary, before, after, detail,
                            error);
                    return getWriterAsync(topicName.getNamespaceObject())
                            .thenCompose(writer -> writer.newMessage()
                                    .key(StringUtils.defaultIfBlank(event.getPartitionedTopic(), event.getTopic()))
                                    .value(event)
                                    .sendAsync())
                            .thenApply(__ -> (Void) null);
                }).exceptionally(ex -> {
                    Throwable actual = FutureUtil.unwrapCompletionException(ex);
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Failed to emit topic trace event {}_{}",
                                topicName, operation.getEventType(), stage, actual);
                    } else {
                        log.warn("[{}] Failed to emit topic trace event {}_{}: {}",
                                topicName, operation.getEventType(), stage, actual.getMessage());
                    }
                    return null;
                });
    }

    @Override
    public void handleEvent(String topic, TopicEvent event, EventStage stage, Throwable t) {
        TopicName topicName = TopicName.get(topic);
        TopicTraceEventType eventType = switch (event) {
            case CREATE -> TopicTraceEventType.CREATE;
            case LOAD -> TopicTraceEventType.LOAD;
            case UNLOAD -> TopicTraceEventType.UNLOAD;
            case DELETE -> TopicTraceEventType.DELETE;
        };
        LifecycleOperationKey key = new LifecycleOperationKey(topicName.getPartitionedTopicName(), eventType);
        String operationId;
        if (stage == EventStage.BEFORE) {
            operationId = newOperationId();
            lifecycleOperations.put(key, operationId);
        } else {
            operationId = lifecycleOperations.remove(key);
            if (operationId == null) {
                operationId = newOperationId();
            }
        }
        TopicTraceOperation operation = new TopicTraceOperation(this, topicName, eventType, TopicTraceContext.system(),
                operationId);
        switch (stage) {
            case BEFORE -> operation.before("Topic lifecycle event", null);
            case SUCCESS -> operation.success("Topic lifecycle event", null);
            case FAILURE -> operation.failure(t, "Topic lifecycle event", null);
            default -> {
            }
        }
    }

    public static TopicName getSystemTopicName(NamespaceName namespaceName) {
        return TopicName.get(TopicDomain.persistent.value(), namespaceName,
                SystemTopicNames.TOPIC_TRACE_EVENTS_LOCAL_NAME);
    }

    public String newOperationId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        writers.forEach((__, writerFuture) -> futures.add(writerFuture.thenCompose(Producer::closeAsync)
                .exceptionally(ex -> null)));
        writers.clear();
        FutureUtil.waitForAll(futures).join();
    }

    private TopicTraceEvent buildEvent(TopicTraceOperation operation, TopicName topicName, TopicTraceStage stage,
                                       String summary, Object before, Object after, Object detail, Throwable error) {
        TopicTraceContext context = operation.getContext();
        return TopicTraceEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .operationId(operation.getOperationId())
                .eventTime(System.currentTimeMillis())
                .cluster(pulsar.getConfiguration().getClusterName())
                .broker(pulsar.getBrokerId())
                .topic(topicName.toString())
                .partitionedTopic(topicName.getPartitionedTopicName())
                .tenant(topicName.getTenant())
                .namespace(topicName.getNamespace())
                .partitionIndex(topicName.isPartitioned() ? topicName.getPartitionIndex() : null)
                .eventType(operation.getEventType())
                .stage(stage)
                .sourceType(context.getSourceType())
                .principal(context.getPrincipal())
                .originalPrincipal(context.getOriginalPrincipal())
                .clientAddress(context.getClientAddress())
                .requestId(context.getRequestId())
                .requestPath(context.getRequestPath())
                .requestMethod(context.getRequestMethod())
                .listenerName(context.getListenerName())
                .subscription(operation.getSubscription())
                .producerName(operation.getProducerName())
                .producerId(operation.getProducerId())
                .consumerName(operation.getConsumerName())
                .consumerId(operation.getConsumerId())
                .ledgerId(operation.getLedgerId())
                .remoteCluster(operation.getRemoteCluster())
                .summary(summary)
                .beforeJson(toJson(before))
                .afterJson(toJson(after))
                .detailJson(toJson(detail))
                .errorClass(error == null ? null : error.getClass().getName())
                .errorMessage(error == null ? null : error.getMessage())
                .build();
    }

    private CompletableFuture<TopicTraceSettings> resolveSettingsAsync(TopicName topicName) {
        if (SystemTopicNames.isSystemTopic(topicName)) {
            return CompletableFuture.completedFuture(TopicTraceSettings.disabled());
        }
        ServiceConfiguration config = pulsar.getConfiguration();
        EnumSet<TopicTraceEventType> brokerEventTypes = parseConfiguredEventTypes(config.getTopicTraceEventTypes());
        CompletableFuture<Map<String, String>> namespacePropertiesFuture = pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPoliciesAsync(topicName.getNamespaceObject())
                .thenApply(policies -> policies.map(p -> p.properties).orElse(Collections.emptyMap()))
                .exceptionally(ex -> Collections.emptyMap());
        CompletableFuture<Map<String, String>> topicPropertiesFuture = getTopicPropertiesAsync(topicName)
                .exceptionally(ex -> Collections.emptyMap());
        return namespacePropertiesFuture.thenCombine(topicPropertiesFuture, (namespaceProperties, topicProperties) -> {
            boolean enabled = applyEnabledOverride(
                    applyEnabledOverride(config.isTopicTraceEnabled(), namespaceProperties), topicProperties);
            EnumSet<TopicTraceEventType> eventTypes = applyEventTypeOverride(
                    applyEventTypeOverride(brokerEventTypes, namespaceProperties), topicProperties);
            return new TopicTraceSettings(enabled, eventTypes);
        });
    }

    private CompletableFuture<Map<String, String>> getTopicPropertiesAsync(TopicName topicName) {
        if (!topicName.isPersistent()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }
        BrokerService brokerService = pulsar.getBrokerService();
        TopicName partitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());
        return brokerService.fetchPartitionedTopicMetadataAsync(partitionedTopicName)
                .thenCompose(metadata -> {
                    if (metadata.partitions > 0) {
                        return CompletableFuture.completedFuture(
                                metadata.properties == null ? Collections.<String, String>emptyMap()
                                        : metadata.properties);
                    }
                    return brokerService.getTopicIfExists(topicName.toString())
                            .thenApply(optionalTopic -> optionalTopic.map(topic -> getManagedLedgerProperties(topic))
                                    .orElse(Collections.<String, String>emptyMap()));
                }).exceptionally(ex -> Collections.<String, String>emptyMap());
    }

    private Map<String, String> getManagedLedgerProperties(Topic topic) {
        if (topic instanceof PersistentTopic persistentTopic) {
            return persistentTopic.getManagedLedger().getProperties();
        }
        return Collections.emptyMap();
    }

    private CompletableFuture<Producer<TopicTraceEvent>> getWriterAsync(NamespaceName namespaceName) {
        return writers.computeIfAbsent(namespaceName, key -> createWriterAsync(key)
                .whenComplete((__, ex) -> {
                    if (ex != null) {
                        writers.remove(key);
                    }
                }));
    }

    private CompletableFuture<Producer<TopicTraceEvent>> createWriterAsync(NamespaceName namespaceName) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("TopicTraceManager is closed"));
        }
        try {
            return pulsar.getClient().newProducer(SCHEMA)
                    .topic(getSystemTopicName(namespaceName).toString())
                    .enableBatching(false)
                    .createAsync();
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private boolean applyEnabledOverride(boolean currentValue, Map<String, String> properties) {
        if (MapUtils.isEmpty(properties) || !properties.containsKey(TOPIC_TRACE_ENABLED_PROPERTY)) {
            return currentValue;
        }
        String value = properties.get(TOPIC_TRACE_ENABLED_PROPERTY);
        if (StringUtils.isBlank(value)) {
            return currentValue;
        }
        return Boolean.parseBoolean(value);
    }

    private EnumSet<TopicTraceEventType> applyEventTypeOverride(EnumSet<TopicTraceEventType> currentValue,
                                                                Map<String, String> properties) {
        if (MapUtils.isEmpty(properties) || !properties.containsKey(TOPIC_TRACE_EVENT_TYPES_PROPERTY)) {
            return currentValue;
        }
        String value = properties.get(TOPIC_TRACE_EVENT_TYPES_PROPERTY);
        if (StringUtils.isBlank(value)) {
            return currentValue;
        }
        return parseConfiguredEventTypes(new LinkedHashSet<>(Arrays.asList(value.split(","))));
    }

    private EnumSet<TopicTraceEventType> parseConfiguredEventTypes(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return EnumSet.allOf(TopicTraceEventType.class);
        }
        EnumSet<TopicTraceEventType> eventTypes = EnumSet.noneOf(TopicTraceEventType.class);
        for (String rawValue : values) {
            if (StringUtils.isBlank(rawValue)) {
                continue;
            }
            for (String token : rawValue.split(",")) {
                if (StringUtils.isBlank(token)) {
                    continue;
                }
                String normalized = token.trim().toUpperCase(Locale.ROOT);
                if ("ALL".equals(normalized) || "*".equals(normalized)) {
                    return EnumSet.allOf(TopicTraceEventType.class);
                }
                try {
                    eventTypes.add(TopicTraceEventType.valueOf(normalized));
                } catch (IllegalArgumentException e) {
                    log.warn("Ignore unknown topic trace event type {}", token);
                }
            }
        }
        return eventTypes.isEmpty() ? EnumSet.allOf(TopicTraceEventType.class) : eventTypes;
    }

    private String toJson(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String stringValue) {
            return stringValue;
        }
        try {
            return objectWriter.writeValueAsString(value);
        } catch (Exception e) {
            return String.valueOf(value);
        }
    }

    private record LifecycleOperationKey(String topicName, TopicTraceEventType eventType) {
    }

    private record TopicTraceSettings(boolean enabled, EnumSet<TopicTraceEventType> eventTypes) {
        static TopicTraceSettings disabled() {
            return new TopicTraceSettings(false, EnumSet.noneOf(TopicTraceEventType.class));
        }

        boolean allows(TopicTraceEventType eventType) {
            return enabled && eventTypes.contains(eventType);
        }
    }
}
