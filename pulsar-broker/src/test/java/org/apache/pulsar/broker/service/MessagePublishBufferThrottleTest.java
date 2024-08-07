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
package org.apache.pulsar.broker.service;

import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;
import static org.apache.pulsar.broker.stats.BrokerOpenTelemetryTestUtil.assertMetricLongSumValue;
import static org.testng.Assert.fail;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.ConnectionRateLimitOperationName;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessagePublishBufferThrottleTest extends BrokerTestBase {

    @Override
    protected void setup() throws Exception {
        //No-op
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        super.customizeMainPulsarTestContextBuilder(pulsarTestContextBuilder);
        pulsarTestContextBuilder.enableOpenTelemetry(true);
    }

    @Test
    public void testMessagePublishBufferThrottleDisabled() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(-1);
        super.baseSetup();
        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleDisabled";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);

        pulsarTestContext.getMockBookKeeper().addEntryDelay(1, TimeUnit.SECONDS);

        // Make sure the producer can publish successfully
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }
        producer.flush();

        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);
    }

    @Test
    public void testMessagePublishBufferThrottleEnable() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testMessagePublishBufferThrottleEnable";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();

        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);

        pulsarTestContext.getMockBookKeeper().addEntryDelay(1, TimeUnit.SECONDS);

        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }

        Awaitility.await().untilAsserted(() -> {
            assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
            assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);
        });

        producer.flush();

        Awaitility.await().untilAsserted(() -> {
            assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
            assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);
        });
    }

    @Test
    public void testBlockByPublishRateLimiting() throws Exception {
        conf.setMaxMessagePublishBufferSizeInMB(1);
        super.baseSetup();

        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);

        final String topic = "persistent://prop/ns-abc/testBlockByPublishRateLimiting";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .producerName("producer-name")
                .create();
        Topic topicRef = pulsar.getBrokerService().getTopicReference(topic).get();
        Assert.assertNotNull(topicRef);
        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);

        pulsarTestContext.getMockBookKeeper().addEntryDelay(5, TimeUnit.SECONDS);

        // Block by publish buffer.
        byte[] payload = new byte[1024 * 1024];
        for (int i = 0; i < 10; i++) {
            producer.sendAsync(payload);
        }

        Awaitility.await().untilAsserted(() -> assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 1));

        CompletableFuture<Void> flushFuture = producer.flushAsync();

        // Block by publish rate.
        // After 1 second, the message buffer throttling will be lifted, but the rate limiting will still be in place.
        assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 1);
        assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 0);

        try {
            flushFuture.get(2, TimeUnit.SECONDS);
            fail("Should have timed out");
        } catch (TimeoutException e) {
            // Ok
        }

        flushFuture.join();

        Awaitility.await().untilAsserted(() -> {
            assertRateLimitCounter(ConnectionRateLimitOperationName.PAUSED, 10);
            assertRateLimitCounter(ConnectionRateLimitOperationName.RESUMED, 10);
        });
    }

    @Test
    public void testConnectionThrottled() throws Exception {
        super.baseSetup();

        var topic = BrokerTestUtil.newUniqueName("persistent://prop/ns-abc/testSendThrottled");

        assertRateLimitCounter(ConnectionRateLimitOperationName.THROTTLED, 0);
        assertRateLimitCounter(ConnectionRateLimitOperationName.UNTHROTTLED, 0);

        @Cleanup
        var producer = pulsarClient.newProducer(Schema.STRING)
                .enableBatching(false)
                .topic(topic)
                .create();
        final int messages = 2000;
        for (int i = 0; i < messages; i++) {
            producer.sendAsync("Message - " + i);
        }
        producer.flush();

        // Wait for the connection to be throttled and unthrottled.
        Awaitility.await().untilAsserted(() -> {
            var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
            assertMetricLongSumValue(metrics, BrokerService.CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME,
                    ConnectionRateLimitOperationName.THROTTLED.attributes, value -> assertThat(value).isPositive());
            assertMetricLongSumValue(metrics, BrokerService.CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME,
                    ConnectionRateLimitOperationName.UNTHROTTLED.attributes, value -> assertThat(value).isPositive());
        });
    }

    private void assertRateLimitCounter(ConnectionRateLimitOperationName connectionRateLimitState, int expectedCount) {
        var metrics = pulsarTestContext.getOpenTelemetryMetricReader().collectAllMetrics();
        if (expectedCount == 0) {
            assertThat(metrics).noneSatisfy(metricData -> assertThat(metricData)
                    .hasName(BrokerService.CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME)
                    .hasLongSumSatisfying(sum -> sum.hasPointsSatisfying(
                            points -> points.hasAttributes(connectionRateLimitState.attributes))));
        } else {
            assertMetricLongSumValue(metrics, BrokerService.CONNECTION_RATE_LIMIT_COUNT_METRIC_NAME,
                    connectionRateLimitState.attributes, expectedCount);
        }
    }
}
