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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntryImplTest {

    @Test
    public void testFailedMetadataInitializationIsNotRetried() {
        ByteBuf bytes = Unpooled.buffer(4).writeInt(-1);
        EntryImpl entry = EntryImpl.create(1, 0, bytes);
        bytes.release();
        entry.data = spy(entry.data);
        try {
            entry.initializeMessageMetadataIfNeeded("ledger");
            entry.initializeMessageMetadataIfNeeded("ledger");
            assertThat(entry.getMessageMetadata()).isNull();
            assertThat(entry.getDataBuffer().readerIndex()).isZero();
            assertThat(entry.getDataBuffer().getInt(0)).isEqualTo(-1);
            verify(entry.data, times(1)).duplicate();
        } finally {
            entry.release();
        }
    }

    @Test
    public void testCreateWithLedgerIdEntryIdAndByteBuf() {
        // Given
        long ledgerId = 123L;
        long entryId = 456L;
        byte[] testData = "test-data".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, data, 1);

        try {
            // Then
            assertEntry(entry, ledgerId, entryId, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateWithPositionAndByteBuf() {
        // Given
        long ledgerId = 789L;
        long entryId = 101L;
        Position position = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "position-test-data".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.create(position, data, 1);

        try {
            // Then
            assertEntryPosition(entry, position);
            assertEntryData(entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateWithRetainedDuplicate() {
        // Given
        long ledgerId = 555L;
        long entryId = 666L;
        Position position = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "retained-duplicate-test".getBytes();
        ByteBuf data = Unpooled.wrappedBuffer(testData);

        // When
        EntryImpl entry = EntryImpl.createWithRetainedDuplicate(position, data, 1);

        try {
            // Then
            assertEntryPosition(entry, position);
            assertEntryData(entry, testData);
            assertRetainedDuplicate(data, entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(data.refCnt(), 1);
    }

    @Test
    public void testCreateFromAnotherEntryImpl() {
        // Given
        long ledgerId = 111L;
        long entryId = 222L;
        byte[] testData = "original-entry-data".getBytes();
        ByteBuf originalData = Unpooled.wrappedBuffer(testData);
        EntryImpl originalEntry = EntryImpl.create(ledgerId, entryId, originalData, 1);

        try {
            // When
            EntryImpl copiedEntry = EntryImpl.create(originalEntry);

            try {
                // Then
                assertEntryPosition(copiedEntry, originalEntry.getPosition());
                assertEntryData(copiedEntry, testData);
                assertRetainedDuplicate(originalData, copiedEntry, testData);
            } finally {
                copiedEntry.release();
            }
        } finally {
            originalEntry.release();
        }

        assertEquals(originalData.refCnt(), 1);
    }

    @Test
    public void testCreateFromGenericEntry() {
        // Given
        long ledgerId = 333L;
        long entryId = 444L;
        Position expectedPosition = PositionFactory.create(ledgerId, entryId);
        byte[] testData = "generic-entry-data".getBytes();
        ByteBuf dataBuffer = Unpooled.wrappedBuffer(testData);

        // Mock Entry interface
        Entry mockEntry = mock(Entry.class);
        when(mockEntry.getPosition()).thenReturn(expectedPosition);
        when(mockEntry.getLedgerId()).thenReturn(ledgerId);
        when(mockEntry.getEntryId()).thenReturn(entryId);
        when(mockEntry.getDataBuffer()).thenReturn(dataBuffer);

        // When
        EntryImpl entry = EntryImpl.create(mockEntry);

        try {
            // Then
            assertEntryPosition(entry, expectedPosition);
            assertEntryData(entry, testData);
            assertRetainedDuplicate(dataBuffer, entry, testData);
        } finally {
            entry.release();
        }

        assertEquals(dataBuffer.refCnt(), 1);
    }

    @Test
    public void testCreateWithEmptyData() {
        // Given
        long ledgerId = 999L;
        long entryId = 0L;
        byte[] emptyData = new byte[0];
        ByteBuf data = Unpooled.EMPTY_BUFFER;

        // When
        EntryImpl entry = EntryImpl.create(ledgerId, entryId, data, 1);

        try {
            // Then
            assertEntry(entry, ledgerId, entryId, emptyData);
        } finally {
            entry.release();
        }
    }


    @Test
    public void testCreateFromEntryImplWhereGetPositionHasntBeenCalled() {
        // Given
        EntryImpl originalEntry = EntryImpl.create(1L, 2L, new byte[0]);
        EntryImpl newEntry = EntryImpl.create(originalEntry);

        // Expect that the position is created lazily and the instances are different
        assertNotSame(originalEntry.getPosition(), newEntry.getPosition());
        assertTrue(originalEntry.matchesPosition(newEntry.getPosition()));

        // Clean up
        originalEntry.release();
        newEntry.release();
    }

    @Test
    public void testCreateFromEntryImplWhereGetPositionHasBeenCalled() {
        // Given
        EntryImpl originalEntry = EntryImpl.create(1L, 2L, new byte[0]);
        originalEntry.getPosition();
        EntryImpl newEntry = EntryImpl.create(originalEntry);

        // Expect that the position instances are the same
        assertSame(originalEntry.getPosition(), newEntry.getPosition());

        // Clean up
        originalEntry.release();
        newEntry.release();
    }

    @Test
    public void testCreateWithPositionThatIsntImmutable() {
        // Given
        Position position = new AckSetPositionImpl(1L, 2L, new long[0]);
        EntryImpl entry = EntryImpl.create(position, Unpooled.EMPTY_BUFFER, 1);

        // Expect that the position is different since it's not immutable
        assertNotSame(entry.getPosition(), position);

        // Clean up
        entry.release();
    }

    @Test
    public void testCreateWithPositionThatIsImmutable() {
        // Given
        Position position = PositionFactory.create(1L, 2L);
        EntryImpl entry = EntryImpl.create(position, Unpooled.EMPTY_BUFFER, 1);

        // Expect that the position is same since it's immutable
        assertSame(entry.getPosition(), position);

        // Clean up
        entry.release();
    }

    @Test
    public void testRecycledObjectDoesNotInheritPoisonedPosition() {
        // Given a legitimate entry that is released normally
        EntryImpl first = EntryImpl.create(5L, 10L, new byte[]{1, 2, 3});
        first.release();

        // When a getPosition() call slips in AFTER the release: deallocation nulls the lazy
        // position field, so this late reader re-materializes it from the reset ids as (-1, -1)
        // and leaves the poisoned value cached inside the pooled object.
        first.getPosition();

        // Then the next create() (the recycler hands back the most recently released object on
        // the same thread) must not report that stale (-1, -1) position as its own — through
        // both the byte[] and the ByteBuf variants, which own the lazy field.
        EntryImpl second = EntryImpl.create(6L, 20L, new byte[]{4, 5, 6});
        assertTrue(second.getPosition().compareTo(PositionFactory.create(6L, 20L)) == 0,
                "byte[] variant: a recycled entry must not inherit the poisoned (-1, -1) position");
        second.release();

        second.getPosition(); // re-poison the recycled object
        EntryImpl third = EntryImpl.create(7L, 30L, Unpooled.wrappedBuffer(new byte[]{7, 8}));
        assertTrue(third.getPosition().compareTo(PositionFactory.create(7L, 30L)) == 0,
                "ByteBuf variant: a recycled entry must not inherit the poisoned (-1, -1) position");
        third.release();
    }

    private enum CreationType {
        LEDGER_ENTRY,
        BYTE_ARRAY,
        BYTE_BUF,
        POSITION,
        RETAINED_DUPLICATE,
        RETAINED_DUPLICATE_WITH_METADATA,
        ENTRY_IMPL,
        ENTRY
    }

    @DataProvider(name = "recycledEntryFactories")
    public Object[][] recycledEntryFactories() {
        CreationType[] types = CreationType.values();
        Object[][] cases = new Object[types.length * 2][];
        for (int i = 0; i < types.length; i++) {
            cases[i * 2] = new Object[]{types[i], false};
            cases[i * 2 + 1] = new Object[]{types[i], true};
        }
        return cases;
    }

    @Test(dataProvider = "recycledEntryFactories")
    public void testRecycledEntryDoesNotInheritPreviousGenerationState(CreationType creationType,
                                                                     boolean cachedMetadata) throws Exception {
        runOnRecyclerThread(() -> {
            MessageMetadata metadata = new MessageMetadata()
                    .setProducerName("new-generation")
                    .setSequenceId(7)
                    .setPublishTime(123456789L);
            ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{4, 5, 6});
            ByteBuf serialized;
            try {
                serialized = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
            } finally {
                payload.release();
            }
            EntryImpl source;
            try {
                source = EntryImpl.create(6L, 20L, serialized, 1);
            } finally {
                serialized.release();
            }
            // The source acts as a cache-owned entry: only the returned read copy counts as a read.
            source.setDecreaseReadCountOnRelease(false);
            try {
                if (cachedMetadata) {
                    source.initializeMessageMetadataIfNeeded("ledger");
                    assertThat(source.getMessageMetadata()).isNotNull();
                }

                EntryImpl previous = getRecyclableEntry();
                previous.release();
                // Reproduce sequential post-release writes through the public methods, before reuse.
                previous.getPosition();
                if (cachedMetadata) {
                    previous.setMessageMetadata(new MessageMetadata().setProducerName("old-generation"));
                } else {
                    // data is null after deallocation; metadata initialization records a failure.
                    previous.initializeMessageMetadataIfNeeded("ledger");
                }
                previous.setDecreaseReadCountOnRelease(false);
                AtomicInteger staleCallbacks = new AtomicInteger();
                previous.onDeallocate(staleCallbacks::incrementAndGet);

                AtomicInteger currentCallbacks = new AtomicInteger();
                EntryImpl current = createEntry(creationType, source);
                EntryReadCountHandlerImpl readCountHandler =
                        (EntryReadCountHandlerImpl) current.getReadCountHandler();
                try {
                    assertThat(current).as("%s must reuse the poisoned instance", creationType).isSameAs(previous);
                    assertThat(current.refCnt()).isEqualTo(1);
                    assertThat(current.getPosition()).isEqualTo(source.getPosition());
                    assertThat(current.getData()).isEqualTo(source.getData());
                    assertThat(readCountHandler.getExpectedReadCount()).isEqualTo(1);
                    current.onDeallocate(currentCallbacks::incrementAndGet);

                    MessageMetadata expectedMetadata = switch (creationType) {
                        case RETAINED_DUPLICATE_WITH_METADATA, ENTRY_IMPL, ENTRY -> source.getMessageMetadata();
                        default -> null;
                    };
                    assertThat(current.getMessageMetadata()).as("metadata before parsing").isSameAs(expectedMetadata);
                    current.initializeMessageMetadataIfNeeded("ledger");
                    assertThat(current.getMessageMetadata()).as("metadata for the new entry").isNotNull();
                    assertThat(current.getMessageMetadata().getProducerName()).isEqualTo("new-generation");
                    assertThat(current.getMessageMetadata().getSequenceId()).isEqualTo(7);
                    assertThat(current.getDataBuffer().readerIndex()).isZero();
                } finally {
                    current.release();
                }
                assertThat(readCountHandler.getExpectedReadCount()).as("the new entry must count its release")
                        .isZero();
                assertThat(currentCallbacks.get()).isEqualTo(1);
                assertThat(staleCallbacks.get()).as("the old generation's callback must not run").isZero();
            } finally {
                source.release();
            }
        });
    }

    private static EntryImpl createEntry(CreationType creationType, EntryImpl source) {
        return switch (creationType) {
            case LEDGER_ENTRY -> {
                try (LedgerEntryImpl ledgerEntry = LedgerEntryImpl.create(source.getLedgerId(), source.getEntryId(),
                        source.getLength(), source.getDataBuffer().retainedDuplicate())) {
                    yield EntryImpl.create(ledgerEntry, 1);
                }
            }
            case BYTE_ARRAY -> EntryImpl.create(source.getLedgerId(), source.getEntryId(), source.getData(), 1);
            case BYTE_BUF -> EntryImpl.create(source.getLedgerId(), source.getEntryId(), source.getDataBuffer(), 1);
            case POSITION -> EntryImpl.create(source.getPosition(), source.getDataBuffer(), 1);
            case RETAINED_DUPLICATE ->
                    EntryImpl.createWithRetainedDuplicate(source.getPosition(), source.getDataBuffer(), 1);
            case RETAINED_DUPLICATE_WITH_METADATA -> EntryImpl.createWithRetainedDuplicate(source.getPosition(),
                    source.getDataBuffer(), source.getReadCountHandler(), source.getMessageMetadata());
            case ENTRY_IMPL -> EntryImpl.create(source);
            case ENTRY -> EntryImpl.create((Entry) source);
        };
    }

    private static EntryImpl getRecyclableEntry() {
        EntryImpl entry = EntryImpl.create(5L, 10L, new byte[]{1, 2, 3});
        for (int i = 0; i < 1024; i++) {
            entry.release();
            EntryImpl next = EntryImpl.create(5L, 10L, new byte[]{1, 2, 3});
            if (next == entry) {
                return next;
            }
            entry = next;
        }
        entry.release();
        throw new AssertionError("EntryImpl recycler did not reuse an instance after warm-up");
    }

    private static void runOnRecyclerThread(Runnable test) throws Exception {
        FutureTask<Void> task = new FutureTask<>(test, null);
        Thread thread = new FastThreadLocalThread(task, "entry-reuse-state-test");
        thread.start();
        try {
            task.get(30, TimeUnit.SECONDS);
        } finally {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }
        assertThat(thread.isAlive()).as("recycler test thread must terminate").isFalse();
    }

    private void assertEntryFields(EntryImpl entry, long expectedLedgerId, long expectedEntryId) {
        assertEquals(entry.getLedgerId(), expectedLedgerId);
        assertEquals(entry.getEntryId(), expectedEntryId);
        assertNotNull(entry.getPosition());
        assertEquals(entry.getPosition().getLedgerId(), expectedLedgerId);
        assertEquals(entry.getPosition().getEntryId(), expectedEntryId);
    }

    private void assertEntryData(EntryImpl entry, byte[] expectedData) {
        byte[] entryData = entry.getData();
        assertEquals(entryData, expectedData);
    }

    private void assertEntry(EntryImpl entry, long expectedLedgerId, long expectedEntryId,
                             byte[] expectedData) {
        assertEntryFields(entry, expectedLedgerId, expectedEntryId);
        assertEntryData(entry, expectedData);
        assertEntryPosition(entry, PositionFactory.create(expectedLedgerId, expectedEntryId));
    }

    private void assertEntryPosition(EntryImpl entry, Position expectedPosition) {
        assertEquals(entry.getLedgerId(), expectedPosition.getLedgerId());
        assertEquals(entry.getEntryId(), expectedPosition.getEntryId());
        assertTrue(entry.getPosition().compareTo(expectedPosition) == 0);
        assertTrue(entry.matchesPosition(expectedPosition));
    }

    private void assertRetainedDuplicate(ByteBuf originalDataBuffer, EntryImpl copiedEntry, byte[] testData) {
        // the new entry's readerIndex should be separate from the original buffer's readerIndex
        // since we created a retained duplicate
        originalDataBuffer.readByte();
        assertEntryData(copiedEntry, testData);
    }
}
