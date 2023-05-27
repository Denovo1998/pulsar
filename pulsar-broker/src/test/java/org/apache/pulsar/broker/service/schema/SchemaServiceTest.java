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
package org.apache.pulsar.broker.service.schema;

import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.SchemaRegistry.SchemaAndMetadata;
import org.apache.pulsar.broker.stats.PrometheusMetricsTest;
import org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGenerator;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class SchemaServiceTest extends MockedPulsarServiceBaseTest {

    private static final Clock MockClock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

    private final String schemaId1 = "1/2/3/4";
    private static final String userId = "user";

    private static final String schemaJson1 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData1 = getSchemaData(schemaJson1);

    private static final String schemaJson2 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\",\"default\":\"foo\"}]}";
    private static final SchemaData schemaData2 = getSchemaData(schemaJson2);

    private static final String schemaJson3 =
            "{\"type\":\"record\",\"name\":\"DefaultTest\",\"namespace\":\"org.apache.pulsar.broker.service.schema" +
                    ".AvroSchemaCompatibilityCheckTest\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}," +
                    "{\"name\":\"field2\",\"type\":\"string\"}]}";
    private static final SchemaData schemaData3 = getSchemaData(schemaJson3);

    private SchemaRegistryServiceImpl schemaRegistryService;
    private BookkeeperSchemaStorage storage;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSchemaRegistryStorageClassName("org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorageFactory");
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
        storage = new BookkeeperSchemaStorage(pulsar);
        storage.start();
        Map<SchemaType, SchemaCompatibilityCheck> checkMap = new HashMap<>();
        checkMap.put(SchemaType.AVRO, new AvroSchemaCompatibilityCheck());
        schemaRegistryService = new SchemaRegistryServiceImpl(storage, checkMap, MockClock, null);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        schemaRegistryService.close();
    }

    @Test
    public void testSchemaRegistryMetrics() throws Exception {
        String schemaId = "tenant/ns/topic" + UUID.randomUUID();
        String namespace = TopicName.get(schemaId).getNamespace();
        putSchema(schemaId, schemaData1, version(0));
        getSchema(schemaId, version(0));
        deleteSchema(schemaId, version(1));

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrometheusMetricsGenerator.generate(pulsar, false, false, false, output);
        output.flush();
        String metricsStr = output.toString(StandardCharsets.UTF_8);
        Multimap<String, PrometheusMetricsTest.Metric> metrics = PrometheusMetricsTest.parseMetrics(metricsStr);

        Collection<PrometheusMetricsTest.Metric> delMetrics = metrics.get("pulsar_schema_del_ops_failed_total");
        Assert.assertEquals(delMetrics.size(), 0);
        Collection<PrometheusMetricsTest.Metric> getMetrics = metrics.get("pulsar_schema_get_ops_failed_total");
        Assert.assertEquals(getMetrics.size(), 0);
        Collection<PrometheusMetricsTest.Metric> putMetrics = metrics.get("pulsar_schema_put_ops_failed_total");
        Assert.assertEquals(putMetrics.size(), 0);

        Collection<PrometheusMetricsTest.Metric> deleteLatency = metrics.get("pulsar_schema_del_ops_latency_count");
        for (PrometheusMetricsTest.Metric metric : deleteLatency) {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        }

        Collection<PrometheusMetricsTest.Metric> getLatency = metrics.get("pulsar_schema_get_ops_latency_count");
        for (PrometheusMetricsTest.Metric metric : getLatency) {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        }

        Collection<PrometheusMetricsTest.Metric> putLatency = metrics.get("pulsar_schema_put_ops_latency_count");
        for (PrometheusMetricsTest.Metric metric : putLatency) {
            Assert.assertEquals(metric.tags.get("namespace"), namespace);
            Assert.assertTrue(metric.value > 0);
        }
    }

    @Test
    public void writeReadBackDeleteSchemaEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());
    }

    @Test
    public void findSchemaVersionTest() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        assertEquals(0, schemaRegistryService.findSchemaVersion(schemaId1, schemaData1).get().longValue());
    }

    @Test
    public void deleteSchemaAndAddSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        SchemaData latest = getLatestSchema(schemaId1, version(0));
        assertEquals(schemaData1, latest);

        deleteSchema(schemaId1, version(1));

        assertNull(schemaRegistryService.getSchema(schemaId1).get());

        putSchema(schemaId1, schemaData1, version(2));

        latest = getLatestSchema(schemaId1, version(2));
        assertEquals(schemaData1, latest);

    }

    @Test
    public void getReturnsTheLastWrittenEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData latest = getLatestSchema(schemaId1, version(1));
        assertEquals(schemaData2, latest);

    }

    @Test
    public void getByVersionReturnsTheCorrectEntry() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry2() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);
    }

    @Test
    public void getByVersionReturnsTheCorrectEntry3() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));

        SchemaData version1 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version1);
    }

    @Test
    public void getAllVersionSchema() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        List<SchemaData> allSchemas = getAllSchemas(schemaId1);
        assertEquals(schemaData1, allSchemas.get(0));
        assertEquals(schemaData2, allSchemas.get(1));
        assertEquals(schemaData3, allSchemas.get(2));
    }

    @Test
    public void addLotsOfEntriesThenDelete() throws Exception {

        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));

        SchemaData version0 = getSchema(schemaId1, version(0));
        assertEquals(schemaData1, version0);

        SchemaData version1 = getSchema(schemaId1, version(1));
        assertEquals(schemaData2, version1);

        SchemaData version2 = getSchema(schemaId1, version(2));
        assertEquals(schemaData3, version2);

        deleteSchema(schemaId1, version(3));

        SchemaRegistry.SchemaAndMetadata version3 = schemaRegistryService.getSchema(schemaId1, version(3)).get();
        assertNull(version3);

    }

    @Test
    public void writeSchemasToDifferentIds() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        String schemaId2 = "id2";
        putSchema(schemaId2, schemaData3, version(0));

        SchemaData withFirstId = getLatestSchema(schemaId1, version(0));
        SchemaData withDifferentId = getLatestSchema(schemaId2, version(0));

        assertEquals(schemaData1, withFirstId);
        assertEquals(schemaData3, withDifferentId);
    }

    @Test
    public void dontReAddExistingSchemaAtRoot() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData1, version(0));
    }

    @Test
    public void trimDeletedSchemaAndGetListTest() throws Exception {
        List<SchemaAndMetadata> list = new ArrayList<>();
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData1, SchemaCompatibilityStrategy.FULL);
        SchemaVersion newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData1, newVersion));
        put = schemaRegistryService.putSchemaIfAbsent(
                schemaId1, schemaData2, SchemaCompatibilityStrategy.FULL);
        newVersion = put.get();
        list.add(new SchemaAndMetadata(schemaId1, schemaData2, newVersion));
        List<SchemaAndMetadata> list1 = schemaRegistryService.trimDeletedSchemaAndGetList(schemaId1).get();
        assertEquals(list.size(), list1.size());
        HashFunction hashFunction = Hashing.sha256();
        for (int i = 0; i < list.size(); i++) {
            SchemaAndMetadata schemaAndMetadata1 = list.get(i);
            SchemaAndMetadata schemaAndMetadata2 = list1.get(i);
            assertEquals(hashFunction.hashBytes(schemaAndMetadata1.schema.getData()).asBytes(),
                    hashFunction.hashBytes(schemaAndMetadata2.schema.getData()).asBytes());
            assertEquals(((LongSchemaVersion)schemaAndMetadata1.version).getVersion()
                    , ((LongSchemaVersion)schemaAndMetadata2.version).getVersion());
            assertEquals(schemaAndMetadata1.id, schemaAndMetadata2.id);
        }
    }

    @Test
    public void dontReAddExistingSchemaInMiddle() throws Exception {
        putSchema(schemaId1, schemaData1, version(0));
        putSchema(schemaId1, schemaData2, version(1));
        putSchema(schemaId1, schemaData3, version(2));
        putSchema(schemaId1, schemaData2, version(1));
    }

    @Test(expectedExceptions = ExecutionException.class)
    public void checkIsCompatible() throws Exception {
        putSchema(schemaId1, schemaData1, version(0), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
        putSchema(schemaId1, schemaData2, version(1), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);

        assertTrue(schemaRegistryService.isCompatible(schemaId1, schemaData3,
                SchemaCompatibilityStrategy.BACKWARD).get());
        assertFalse(schemaRegistryService.isCompatible(schemaId1, schemaData3,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE).get());
        putSchema(schemaId1, schemaData3, version(2), SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE);
    }

    @Test
    public void testSchemaStorageFailed() throws Exception {
        conf.setSchemaRegistryStorageClassName("Unknown class name");
        try {
            restartBroker();
            fail("An exception should have been thrown");
        } catch (Exception rte) {
            Throwable e = rte.getCause();
            Assert.assertEquals(e.getClass(), PulsarServerException.class);
            Assert.assertTrue(e.getMessage().contains("User class must be in class path"));
        }
    }

    @Test(dataProvider = "lostSchemaLedgerIndexes", timeOut = 30000)
    public void testSchemaLedgerLost(List<Integer> lostSchemaLedgerIndexes) throws Exception {
        final String namespace = "public/default";
        final String topic = namespace + "/testSchemaLedgerLost";
        final Schema<V1Data> schemaV1 = Schema.AVRO(V1Data.class);
        final Schema<V2Data> schemaV2 = Schema.AVRO(V2Data.class);
        admin.namespaces().setSchemaCompatibilityStrategy(namespace, SchemaCompatibilityStrategy.BACKWARD);
        admin.topics().createNonPartitionedTopic(topic);
        admin.topics().setSchemaValidationEnforced(topic, true);

        Producer<V1Data> producer1 = pulsarClient.newProducer(schemaV1)
                .topic(topic)
                .producerName("producer1")
                .create();
        Producer<V2Data> producer2 = pulsarClient.newProducer(schemaV2)
                .topic(topic)
                .producerName("producer2")
                .create();
        Consumer<V1Data> consumer1 = pulsarClient.newConsumer(schemaV1)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub0")
                .consumerName("consumer1")
                .subscribe();
        // @Cleanup
        // Consumer<V2Data> consumer2 = pulsarClient.newConsumer(schemaV2)
        //         .topic(topic)
        //         .subscriptionType(SubscriptionType.Shared)
        //         .subscriptionName("sub0")
        //         .consumerName("consumerAfterLostLedger2")
        //         .subscribe();

        SchemaAndMetadata schemaAndMetadata0 = schemaRegistryService.getSchema(TopicName.get(topic)
                .getSchemaName(), new LongSchemaVersion(0)).get();
        SchemaAndMetadata schemaAndMetadata1 = schemaRegistryService.getSchema(TopicName.get(topic)
                .getSchemaName(), new LongSchemaVersion(1)).get();

        // delete ledger
        String key = TopicName.get(topic).getSchemaName();
        List<Long> schemaLedgerList = storage.getSchemaLedgerList(key);
        Assert.assertEquals(schemaLedgerList.size(), 2);
        for (int i = 0; i < schemaLedgerList.size(); i++){
            if (lostSchemaLedgerIndexes.contains(i)){
                storage.getBookKeeper().deleteLedger(schemaLedgerList.get(i));
            }
        }

        // Without introducing this pr, connected producers or consumers are not affected if the schema ledger is lost
        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            producer1.send(new V1Data(i));
            producer2.send(new V2Data(i, i + 1));
        }
        for (int i = 0; i < numMessages; i++) {
            Message<V1Data> msg = consumer1.receive(3, TimeUnit.SECONDS);
            consumer1.acknowledge(msg);
        }

        // try to fix the lost schema ledger
        if (lostSchemaLedgerIndexes.contains(0) && lostSchemaLedgerIndexes.contains(1)) {
            schemaRegistryService.tryCompleteTheLostSchema(TopicName.get(topic)
                            .getSchemaName(), new LongSchemaVersion(0)
                    , schemaAndMetadata0.schema);
            // TODO: BadVersion for /schemas/public/default/testSchemaLedgerLost. Need to fix.
            //  When lostSchemaLedgerIndexes contains 0 and 1.
            schemaRegistryService.tryCompleteTheLostSchema(TopicName.get(topic)
                            .getSchemaName(), new LongSchemaVersion(1)
                    , schemaAndMetadata1.schema);
        } else if (lostSchemaLedgerIndexes.contains(0) && !lostSchemaLedgerIndexes.contains(1)) {
            schemaRegistryService.tryCompleteTheLostSchema(TopicName.get(topic)
                            .getSchemaName(), new LongSchemaVersion(0)
                    , schemaAndMetadata0.schema);
        } else if (!lostSchemaLedgerIndexes.contains(0) && lostSchemaLedgerIndexes.contains(1)) {
            schemaRegistryService.tryCompleteTheLostSchema(TopicName.get(topic)
                            .getSchemaName(), new LongSchemaVersion(1)
                    , schemaAndMetadata1.schema);
        }

        @Cleanup
        Producer<V1Data> producerAfterLostLedger1 = pulsarClient.newProducer(schemaV1)
                .topic(topic)
                .producerName("producerAfterLostLedger1")
                .create();
        assertNotNull(producerAfterLostLedger1.send(new V1Data(10)));
        @Cleanup
        Producer<V2Data> producerAfterLostLedger2 = pulsarClient.newProducer(schemaV2)
                .topic(topic)
                .producerName("producerAfterLostLedger2")
                .create();
        assertNotNull(producerAfterLostLedger2.send(new V2Data(10, 10)));

        @Cleanup
        Consumer<V1Data> consumerAfterLostLedger1 = pulsarClient.newConsumer(schemaV1)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub1")
                .consumerName("consumerAfterLostLedger1")
                .subscribe();
        producer1.send(new V1Data(11));
        assertNotNull(consumerAfterLostLedger1.receive(3, TimeUnit.SECONDS));

        @Cleanup
        Consumer<V2Data> consumerAfterLostLedger2 = pulsarClient.newConsumer(schemaV2)
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub0")
                .consumerName("consumerAfterLostLedger2")
                .subscribe();
        producer2.send(new V2Data(11, 11));
        assertNotNull(consumerAfterLostLedger2.receive(3, TimeUnit.SECONDS));

        producer1.close();
        producer2.close();
        consumer1.close();
    }

    @DataProvider(name = "lostSchemaLedgerIndexes")
    public Object[][] lostSchemaLedgerIndexes(){
        return new Object[][]{
                // {Arrays.asList(0,1)},
                {Arrays.asList(0)},
                {Arrays.asList(1)}
        };
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V1Data {
        int i;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V2Data {
        int i;
        Integer j;
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion) throws Exception {
        putSchema(schemaId, schema, expectedVersion, SchemaCompatibilityStrategy.FULL);
    }

    private void putSchema(String schemaId, SchemaData schema, SchemaVersion expectedVersion,
                           SchemaCompatibilityStrategy strategy) throws ExecutionException, InterruptedException {
        CompletableFuture<SchemaVersion> put = schemaRegistryService.putSchemaIfAbsent(
                schemaId, schema, strategy);
        SchemaVersion newVersion = put.get();
        assertEquals(expectedVersion, newVersion);
    }

    private SchemaData getLatestSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId).get();
        assertEquals(expectedVersion, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private SchemaData getSchema(String schemaId, SchemaVersion version) throws Exception {
        SchemaRegistry.SchemaAndMetadata schemaAndVersion = schemaRegistryService.getSchema(schemaId, version).get();
        assertEquals(version, schemaAndVersion.version);
        assertEquals(schemaId, schemaAndVersion.id);
        return schemaAndVersion.schema;
    }

    private List<SchemaData> getAllSchemas(String schemaId) throws Exception {
        List<SchemaData> result = new ArrayList<>();
        for (CompletableFuture<SchemaRegistry.SchemaAndMetadata> schema :
                schemaRegistryService.getAllSchemas(schemaId).get()) {
            result.add(schema.get().schema);
        }
        return result;
    }

    private void deleteSchema(String schemaId, SchemaVersion expectedVersion) throws Exception {
        SchemaVersion version = schemaRegistryService.deleteSchema(schemaId, userId, false).get();
        assertEquals(expectedVersion, version);
    }

    private SchemaData randomSchema() {
        UUID randomString = UUID.randomUUID();
        return SchemaData.builder()
            .user(userId)
            .type(SchemaType.JSON)
            .timestamp(MockClock.millis())
            .isDeleted(false)
            .data(randomString.toString().getBytes())
            .props(new TreeMap<>())
            .build();
    }

    private static SchemaData getSchemaData(String schemaJson) {
        return SchemaData.builder().data(schemaJson.getBytes()).type(SchemaType.AVRO).user(userId).build();
    }

    private SchemaVersion version(long version) {
        return new LongSchemaVersion(version);
    }
}
