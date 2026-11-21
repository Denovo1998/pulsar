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

import static org.apache.pulsar.metadata.impl.ZKMetadataStoreTest.withDisconnectedSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.CustomLog;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.DualMetadataStore;
import org.apache.pulsar.utils.TestLogAppender;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker")
public class BrokerServiceChaosTest extends CanReconnectZKClientPulsarServiceBaseTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        config.setTopicPoliciesServiceClassName(MetadataStoreTopicPoliciesService.class.getName());
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @DataProvider
    public Object[][] offlinePolicyChanges() {
        return new Object[][] {
                {false, false, false, "persistent"},
                {true, false, false, "non-persistent"},
                {false, true, false, "persistent"},
                {true, false, true, "persistent"}
        };
    }

    @Test(dataProvider = "offlinePolicyChanges")
    public void testLoadedTopicRecoversMissedPolicy(boolean global, boolean delete, boolean expire, String domain)
            throws Exception {
        TopicName name = TopicName.get(domain + "://" + defaultNamespace + "/"
                + BrokerTestUtil.newUniqueName("policy-reconnect"));
        String topic = name.toString();
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(topic).create();
        Topic loaded = broker.getTopicReference(topic).orElseThrow();
        var ttl = loaded.getHierarchyTopicPolicies().getMessageTTLInSeconds();
        if (delete) {
            admin.topicPolicies(true).setMessageTTL(topic, 60);
            Awaitility.await().untilAsserted(() -> assertThat(ttl.get()).isEqualTo(60));
        }
        admin.topicPolicies(global).setMessageTTL(topic, 10);
        Awaitility.await().untilAsserted(() -> assertThat(ttl.get()).isEqualTo(10));

        String url = global ? config.getConfigurationMetadataStoreUrl() : config.getMetadataStoreUrl();
        try (MetadataStore writer = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build())) {
            var cache = writer.getMetadataCache(TopicPolicies.class);
            String path = MetadataStoreTopicPoliciesService.pathFor(name, global);
            MetadataStoreExtended store = global ? (MetadataStoreExtended) pulsar.getConfigurationMetadataStore()
                    : pulsar.getLocalMetadataStore();
            withDisconnectedSession(store, url, expire, () -> {
                if (delete) {
                    cache.delete(path).get(10, TimeUnit.SECONDS);
                } else {
                    cache.readModifyUpdate(path, policies -> {
                        policies.setMessageTTLInSeconds(20);
                        return policies;
                    }).get(10, TimeUnit.SECONDS);
                }
                assertThat(ttl.get()).isEqualTo(10);
            });
        }

        // No admin read, topic reload or subsequent write may be needed to observe the final policy.
        Awaitility.await().untilAsserted(() -> assertThat(ttl.get()).isEqualTo(delete ? 60 : 20));
        assertThat(broker.getTopicReference(topic).orElseThrow()).isSameAs(loaded);
    }

    @DataProvider
    public Object[][] booleans() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "booleans")
    public void testReconnectReportsOnlyAnActualGlobalDeletion(boolean hadGlobalPolicy) throws Exception {
        TopicName name = TopicName.get(defaultNamespace + "/" + BrokerTestUtil.newUniqueName("local-policy"));
        var configStore = (MetadataStoreExtended) pulsar.getConfigurationMetadataStore();
        String url = config.getConfigurationMetadataStoreUrl();
        String path = MetadataStoreTopicPoliciesService.pathFor(name, true);
        try (var service = new MetadataStoreTopicPoliciesService();
             MetadataStore writer = MetadataStoreFactory.create(url, MetadataStoreConfig.builder().build())) {
            var cache = writer.getMetadataCache(TopicPolicies.class);
            if (hadGlobalPolicy) {
                // Create the record before registering either listener.
                var created = new CompletableFuture<Void>();
                Runnable unregister = configStore.registerCancellableListener(event -> {
                    if (event.getType() == NotificationType.Created && path.equals(event.getPath())) {
                        created.complete(null);
                    }
                });
                try {
                    cache.create(path, TopicPolicies.builder().isGlobal(true).messageTTLInSeconds(60).build())
                            .get(10, TimeUnit.SECONDS);
                    created.get(10, TimeUnit.SECONDS);
                } finally {
                    unregister.run();
                }
            }
            service.start(pulsar);
            var observed = new CopyOnWriteArrayList<TopicPolicies>();
            service.registerListenerAsync(name, observed::add).get(10, TimeUnit.SECONDS);
            // Observe completion of the reconciliation on the same topic's notification thread.
            var reconciled = new CompletableFuture<Void>();
            TopicPolicyListenerWrapper scoped = new TopicPolicyListenerWrapper(policy -> {
                if (policy.isGlobalPolicies() && policy.getMessageTTLInSeconds() == null) {
                    reconciled.complete(null);
                }
            }, name.toString());
            scoped.completeInitialization(null, null);
            service.registerListenerAsync(name, scoped).get(10, TimeUnit.SECONDS);
            service.updateTopicPoliciesAsync(name, false, false, policy -> policy.setMessageTTLInSeconds(10))
                    .get(10, TimeUnit.SECONDS);
            TopicPolicies local = TopicPolicies.builder().messageTTLInSeconds(10).build();
            Awaitility.await().untilAsserted(() -> assertThat(observed).contains(local));

            withDisconnectedSession(configStore, url, false, () -> {
                if (hadGlobalPolicy) {
                    cache.delete(path).get(10, TimeUnit.SECONDS);
                }
            });
            reconciled.get(10, TimeUnit.SECONDS);
            if (hadGlobalPolicy) {
                assertThat(observed).last().isNull();
            } else {
                assertThat(observed).containsOnly(local);
            }
        }
    }

    @Test
    public void testPolicyRefreshRetriesAfterReadFailure() throws Exception {
        TopicName name = TopicName.get(defaultNamespace + "/" + BrokerTestUtil.newUniqueName("policy-retry"));
        @Cleanup
        Producer<byte[]> producer = client.newProducer().topic(name.toString()).create();
        var ttl = broker.getTopicReference(name.toString()).orElseThrow()
                .getHierarchyTopicPolicies().getMessageTTLInSeconds();
        admin.topicPolicies().setMessageTTL(name.toString(), 10);
        Awaitility.await().untilAsserted(() -> assertThat(ttl.get()).isEqualTo(10));

        String path = MetadataStoreTopicPoliciesService.pathFor(name, false);
        String connectString = config.getMetadataStoreUrl().substring("zk:".length());
        try (ZooKeeper writer = new ZooKeeper(connectString, 10_000, event -> {});
             TestLogAppender logs = TestLogAppender.create(MetadataStoreTopicPoliciesService.class)) {
            // setACL does not generate a data watch. Restoring read access alone must let the retry converge.
            try {
                withDisconnectedSession(pulsar.getLocalMetadataStore(), config.getMetadataStoreUrl(), false, () -> {
                    writer.setData(path, ObjectMapperFactory.getMapper().writer()
                            .writeValueAsBytes(TopicPolicies.builder().messageTTLInSeconds(20).build()), -1);
                    writer.setACL(path, Collections.singletonList(
                            new ACL(ZooDefs.Perms.ALL & ~ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE)), -1);
                });
                Awaitility.await().until(() -> logs.getEvents().stream()
                        .filter(e -> e.getLoggerName().equals(MetadataStoreTopicPoliciesService.class.getName()))
                        .anyMatch(e -> e.getMessage().getFormattedMessage().contains("Failed to refresh")));
                assertThat(ttl.get()).isEqualTo(10);
            } finally {
                writer.setACL(path, ZooDefs.Ids.OPEN_ACL_UNSAFE, -1);
            }
            Awaitility.await().untilAsserted(() -> assertThat(ttl.get()).isEqualTo(20));
        }
    }

    @Test(dataProvider = "booleans")
    public void testPolicyReadCompletionOrder(boolean releaseFirst) throws Exception {
        TopicName name = TopicName.get(defaultNamespace + "/" + BrokerTestUtil.newUniqueName("policy-order"));
        var readTwenty = new CompletableFuture<Void>();
        var releaseTwenty = new CompletableFuture<Void>();
        // ZooKeeper serves reads on a connection in order. Delay delivery of the real first result to
        // exercise the order in which asynchronous completions can reach the topic notification thread.
        try (var service = new MetadataStoreTopicPoliciesService() {
            @Override
            public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topic, GetType type) {
                return super.getTopicPoliciesAsync(topic, type).thenCompose(value -> {
                    if (value.map(TopicPolicies::getMessageTTLInSeconds).orElse(0) == 20) {
                        readTwenty.complete(null);
                        return releaseTwenty.thenApply(__ -> value);
                    }
                    return CompletableFuture.completedFuture(value);
                });
            }
        }; MetadataStore writer = MetadataStoreFactory.create(config.getMetadataStoreUrl(),
                MetadataStoreConfig.builder().build())) {
            service.start(pulsar);
            List<Integer> observed = new CopyOnWriteArrayList<>();
            service.registerListenerAsync(name, p -> observed.add(p.getMessageTTLInSeconds()))
                    .get(10, TimeUnit.SECONDS);
            String path = MetadataStoreTopicPoliciesService.pathFor(name, false);
            var cache = writer.getMetadataCache(TopicPolicies.class);
            cache.create(path, TopicPolicies.builder().messageTTLInSeconds(20).build()).get(10, TimeUnit.SECONDS);
            readTwenty.get(10, TimeUnit.SECONDS);
            // A listener registered after this read has started loads its own initial value.
            List<Integer> laterListener = new CopyOnWriteArrayList<>();
            service.registerListenerAsync(name, p -> laterListener.add(p.getMessageTTLInSeconds()))
                    .get(10, TimeUnit.SECONDS);
            if (releaseFirst) {
                releaseTwenty.complete(null);
                Awaitility.await().untilAsserted(() -> assertThat(observed).contains(20));
            }
            cache.readModifyUpdate(path, p -> {
                p.setMessageTTLInSeconds(30);
                return p;
            }).get(10, TimeUnit.SECONDS);
            Awaitility.await().untilAsserted(() -> assertThat(observed).contains(30));
            releaseTwenty.complete(null);
            broker.getTopicPoliciesNotifyThread(name).submit(() -> {}).get(10, TimeUnit.SECONDS);
            assertThat(observed).containsExactlyElementsOf(releaseFirst ? List.of(20, 30) : List.of(30));
            assertThat(laterListener).containsExactly(30);
        } finally {
            releaseTwenty.complete(null);
        }
    }

    @Test
    public void testFetchPartitionedTopicMetadataWithCacheRefresh() throws Exception {
        final String configMetadataStoreConnectString =
                WhiteboxImpl.getInternalState(
                        ((DualMetadataStore) pulsar.getConfigurationMetadataStore()).getSourceStore(),
                        "zkConnectString");
        @Cleanup
        final ZooKeeper anotherZKCli = new ZooKeeper(configMetadataStoreConnectString, 5000, null);
        // Set policy of auto create topic to PARTITIONED.
        final String ns = BrokerTestUtil.newUniqueName(defaultTenant + "/ns");
        final TopicName topicName1 = TopicName.get("persistent://" + ns + "/tp1");
        final TopicName topicName2 = TopicName.get("persistent://" + ns + "/tp2");
        admin.namespaces().createNamespace(ns);
        AutoTopicCreationOverride autoTopicCreationOverride =
                new AutoTopicCreationOverrideImpl.AutoTopicCreationOverrideImplBuilder().allowAutoTopicCreation(true)
                                .topicType(TopicType.PARTITIONED.toString())
                                .defaultNumPartitions(3).build();
        admin.namespaces().setAutoTopicCreationAsync(ns, autoTopicCreationOverride);
        // Make the cache of namespace policy is valid.
        admin.namespaces().getAutoSubscriptionCreation(ns);
        // Trigger the zk node "/admin/partitioned-topics/{namespace}/persistent" created.
        admin.topics().createPartitionedTopic(topicName1.toString(), 2);
        admin.topics().deletePartitionedTopic(topicName1.toString());

        // Since there is no partitioned metadata created, the partitions count of metadata will be 0.
        PartitionedTopicMetadata partitionedTopicMetadata1 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2).get();
        assertEquals(partitionedTopicMetadata1.partitions, 0);

        // Create the partitioned metadata by another zk client.
        // Make a error to make the cache could not update.
        startLocalMetadataStoreConnectionTermination();
        anotherZKCli.create("/admin/partitioned-topics/" + ns + "/persistent/" + topicName2.getLocalName(),
                "{\"partitions\":3}".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        stopLocalMetadataStoreConnectionTermination();

        // Get the partitioned metadata from cache, there is 90% chance that partitions count of metadata is 0.
        PartitionedTopicMetadata partitionedTopicMetadata2 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2).get();
        // Note: If you want to reproduce the issue, you can perform validation on the next line.
        // assertEquals(partitionedTopicMetadata2.partitions, 0);

        // Verify the new method will return a correct result.
        PartitionedTopicMetadata partitionedTopicMetadata3 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2, true).get();
        assertEquals(partitionedTopicMetadata3.partitions, 3);

        // cleanup.
        stopLocalMetadataStoreConnectionTermination();
    }
}
