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
package org.apache.pulsar.broker.service.persistent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator.InFlightTask;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.annotations.Test;

@Test(groups = "broker-replication")
public class GeoPersistentReplicatorTest {

    @Test
    public void testSchemaInfoSynchronousFailureCompletesCurrentEntry() throws Exception {
        ThrowingSchemaReplicator replicator = new ThrowingSchemaReplicator();
        replicator.forceStarted();

        Position position = PositionFactory.create(1, 2);
        ByteBuf headersAndPayload = newMessageWithSchemaVersion();
        Entry entry = mock(Entry.class);
        when(entry.getLength()).thenReturn(headersAndPayload.readableBytes());
        when(entry.getDataBuffer()).thenReturn(headersAndPayload);
        when(entry.getPosition()).thenReturn(position);
        when(entry.getLedgerId()).thenReturn(position.getLedgerId());
        when(entry.getEntryId()).thenReturn(position.getEntryId());
        doAnswer(invocation -> {
            headersAndPayload.release();
            return null;
        }).when(entry).release();

        List<Entry> entries = List.of(entry);
        InFlightTask inFlightTask = new InFlightTask(position, 1, replicator.getReplicatorId());
        inFlightTask.setEntries(entries);

        try {
            assertThat(replicator.replicateEntries(entries, inFlightTask)).isFalse();

            assertThat(inFlightTask.getCompletedEntries())
                    .as("the failed entry should release its in-flight permit")
                    .isEqualTo(1);
            verify(entry).release();
        } finally {
            while (headersAndPayload.refCnt() > 0) {
                headersAndPayload.release();
            }
        }
    }

    private static ByteBuf newMessageWithSchemaVersion() {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("producer")
                .setSequenceId(1)
                .setPublishTime(System.currentTimeMillis())
                .setSchemaVersion(new byte[] { 1 });
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[] { 1 });
        try {
            return Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
        } finally {
            payload.release();
        }
    }

    private static class ThrowingSchemaReplicator extends GeoPersistentReplicator {

        @SuppressWarnings("unchecked")
        ThrowingSchemaReplicator() throws PulsarServerException {
            this(mockTopic(), mockCursor(), "local", "remote",
                    mockBrokerService(), mockReplicationClient(), mock(PulsarAdmin.class));
        }

        private ThrowingSchemaReplicator(PersistentTopic topic, ManagedCursor cursor, String localCluster,
                                         String remoteCluster, BrokerService brokerService,
                                         PulsarClientImpl replicationClient, PulsarAdmin replicationAdmin)
                throws PulsarServerException {
            super(topic, cursor, localCluster, remoteCluster, brokerService, replicationClient, replicationAdmin);
        }

        @Override
        protected void startProducer() {
            // Avoid creating a real remote producer from the superclass constructor.
        }

        @Override
        protected CompletableFuture<SchemaInfo> getSchemaInfo(MessageImpl msg) throws ExecutionException {
            throw new ExecutionException(new RuntimeException("injected schema provider failure"));
        }

        void forceStarted() {
            STATE_UPDATER.set(this, State.Started);
            this.producer = mock(ProducerImpl.class);
        }

        private static PersistentTopic mockTopic() throws PulsarServerException {
            PersistentTopic topic = mock(PersistentTopic.class);
            BrokerService brokerService = mockBrokerService();
            when(topic.getName()).thenReturn("persistent://public/default/t1");
            when(topic.getBrokerService()).thenReturn(brokerService);
            when(topic.getReplicatorPrefix()).thenReturn("pulsar.repl");
            when(topic.getReplicatorDispatchRate()).thenReturn(null);
            return topic;
        }

        private static ManagedCursor mockCursor() {
            ManagedCursor cursor = mock(ManagedCursor.class);
            when(cursor.getName()).thenReturn("pulsar.repl.remote");
            return cursor;
        }

        private static BrokerService mockBrokerService() throws PulsarServerException {
            ServiceConfiguration config = new ServiceConfiguration();
            PulsarService pulsar = mock(PulsarService.class);
            BrokerService brokerService = mock(BrokerService.class);
            PulsarClientImpl localClient = mock(PulsarClientImpl.class);
            PulsarAdmin admin = mock(PulsarAdmin.class);

            when(pulsar.getConfiguration()).thenReturn(config);
            when(pulsar.getConfig()).thenReturn(config);
            when(pulsar.getClient()).thenReturn(localClient);
            when(pulsar.getAdminClient()).thenReturn(admin);
            when(brokerService.pulsar()).thenReturn(pulsar);
            when(brokerService.getPulsar()).thenReturn(pulsar);
            return brokerService;
        }

        @SuppressWarnings("unchecked")
        private static PulsarClientImpl mockReplicationClient() {
            PulsarClientImpl replicationClient = mock(PulsarClientImpl.class);
            ProducerBuilder<byte[]> producerBuilder = mock(ProducerBuilder.class, RETURNS_SELF);
            when(replicationClient.newProducer(any(Schema.class))).thenReturn(producerBuilder);
            return replicationClient;
        }
    }
}
