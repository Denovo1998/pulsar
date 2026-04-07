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
package org.apache.pulsar.broker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.service.trace.TopicTraceEvent;
import org.apache.pulsar.broker.service.trace.TopicTraceEventType;
import org.apache.pulsar.broker.service.trace.TopicTraceManager;
import org.apache.pulsar.broker.service.trace.TopicTraceSourceType;
import org.apache.pulsar.broker.service.trace.TopicTraceStage;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TopicTraceManagerTest extends BrokerTestBase {

    private static final Schema<TopicTraceEvent> TRACE_SCHEMA = Schema.AVRO(TopicTraceEvent.class);

    private String namespace;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setTopicTraceEnabled(true);
        super.baseSetup();
        namespace = "prop/topic-trace-" + UUID.randomUUID();
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTraceEventsCapturedForTopicOperations() throws Exception {
        String topic = "persistent://" + namespace + "/topic-" + UUID.randomUUID();
        @Cleanup
        Reader<TopicTraceEvent> reader = newTraceReader();

        admin.topics().createNonPartitionedTopic(topic);
        admin.lookups().lookupTopic(topic);

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("trace-producer")
                .create();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("trace-sub")
                .consumerName("trace-consumer")
                .subscribe();

        producer.send("first");
        producer.send("second");

        admin.topics().updateProperties(topic, Map.of("env", "test"));
        admin.topics().createSubscription(topic, "admin-sub", MessageId.earliest);
        admin.topics().resetCursor(topic, "admin-sub", MessageId.earliest);
        admin.topics().skipAllMessages(topic, "admin-sub");
        admin.topics().deleteSubscription(topic, "admin-sub");

        consumer.close();
        producer.close();

        List<TopicTraceEvent> events = readUntil(reader, currentEvents ->
                findEvent(currentEvents, topic, TopicTraceEventType.CREATE, TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.LOAD, TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.LOOKUP, TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.PRODUCER_CONNECT,
                        TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.PRODUCER_DISCONNECT,
                        TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.CONSUMER_CONNECT,
                        TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.CONSUMER_DISCONNECT,
                        TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.TOPIC_METADATA_UPDATE,
                        TopicTraceStage.SUCCESS) != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.SUBSCRIPTION_CREATE,
                        TopicTraceStage.SUCCESS, "admin-sub") != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.SUBSCRIPTION_SEEK,
                        TopicTraceStage.SUCCESS, "admin-sub") != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.SUBSCRIPTION_CLEAR_BACKLOG,
                        TopicTraceStage.SUCCESS, "admin-sub") != null
                        && findEvent(currentEvents, topic, TopicTraceEventType.SUBSCRIPTION_DELETE,
                        TopicTraceStage.SUCCESS, "admin-sub") != null);

        TopicTraceEvent lookupEvent = findEvent(events, topic, TopicTraceEventType.LOOKUP, TopicTraceStage.SUCCESS);
        assertNotNull(lookupEvent);
        assertEquals(lookupEvent.getSourceType(), TopicTraceSourceType.HTTP);

        TopicTraceEvent producerConnectEvent =
                findEvent(events, topic, TopicTraceEventType.PRODUCER_CONNECT, TopicTraceStage.SUCCESS);
        assertNotNull(producerConnectEvent);
        assertEquals(producerConnectEvent.getProducerName(), "trace-producer");

        TopicTraceEvent consumerConnectEvent =
                findEvent(events, topic, TopicTraceEventType.CONSUMER_CONNECT, TopicTraceStage.SUCCESS);
        assertNotNull(consumerConnectEvent);
        assertEquals(consumerConnectEvent.getSubscription(), "trace-sub");
        assertEquals(consumerConnectEvent.getConsumerName(), "trace-consumer");

        TopicTraceEvent metadataUpdateEvent =
                findEvent(events, topic, TopicTraceEventType.TOPIC_METADATA_UPDATE, TopicTraceStage.SUCCESS);
        assertNotNull(metadataUpdateEvent);
        assertEquals(metadataUpdateEvent.getSummary(), "Topic metadata updated");
        assertTrue(metadataUpdateEvent.getDetailJson().contains("\"env\":\"test\""));
    }

    @Test
    public void testTopicPropertyCanDisableTraceEvents() throws Exception {
        String topic = "persistent://" + namespace + "/topic-" + UUID.randomUUID();
        @Cleanup
        Reader<TopicTraceEvent> reader = newTraceReader();

        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().updateProperties(topic, Map.of(TopicTraceManager.TOPIC_TRACE_ENABLED_PROPERTY, "false"));

        Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic(topic)
                .producerName("disabled-producer")
                .create();
        producer.close();

        List<TopicTraceEvent> events = readUntil(reader, currentEvents ->
                findEvent(currentEvents, topic, TopicTraceEventType.TOPIC_METADATA_UPDATE,
                        TopicTraceStage.BEFORE) != null);
        events.addAll(drain(reader, 1500));

        assertNotNull(findEvent(events, topic, TopicTraceEventType.TOPIC_METADATA_UPDATE, TopicTraceStage.BEFORE));
        assertFalse(containsEvent(events, topic, TopicTraceEventType.PRODUCER_CONNECT));
        assertFalse(containsEvent(events, topic, TopicTraceEventType.PRODUCER_DISCONNECT));
    }

    private Reader<TopicTraceEvent> newTraceReader() throws Exception {
        return pulsarClient.newReader(TRACE_SCHEMA)
                .topic(traceTopicName())
                .startMessageId(MessageId.latest)
                .create();
    }

    private String traceTopicName() {
        return "persistent://" + namespace + "/" + SystemTopicNames.TOPIC_TRACE_EVENTS_LOCAL_NAME;
    }

    private List<TopicTraceEvent> readUntil(Reader<TopicTraceEvent> reader, Predicate<List<TopicTraceEvent>> done) {
        List<TopicTraceEvent> events = new ArrayList<>();
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
            events.addAll(drain(reader, 500));
            return done.test(events);
        });
        return events;
    }

    private List<TopicTraceEvent> drain(Reader<TopicTraceEvent> reader, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        List<TopicTraceEvent> events = new ArrayList<>();
        while (System.nanoTime() < deadline) {
            Message<TopicTraceEvent> message;
            try {
                message = reader.readNext(200, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (message == null) {
                continue;
            }
            try {
                events.add(message.getValue());
            } finally {
                message.release();
            }
        }
        return events;
    }

    private boolean containsEvent(List<TopicTraceEvent> events, String topic, TopicTraceEventType eventType) {
        return events.stream()
                .anyMatch(event -> topic.equals(event.getTopic()) && eventType == event.getEventType());
    }

    private TopicTraceEvent findEvent(List<TopicTraceEvent> events, String topic, TopicTraceEventType eventType,
                                      TopicTraceStage stage) {
        return findEvent(events, topic, eventType, stage, null);
    }

    private TopicTraceEvent findEvent(List<TopicTraceEvent> events, String topic, TopicTraceEventType eventType,
                                      TopicTraceStage stage, String subscription) {
        return events.stream()
                .filter(event -> topic.equals(event.getTopic()))
                .filter(event -> eventType == event.getEventType())
                .filter(event -> stage == event.getStage())
                .filter(event -> subscription == null || subscription.equals(event.getSubscription()))
                .findFirst()
                .orElse(null);
    }
}
