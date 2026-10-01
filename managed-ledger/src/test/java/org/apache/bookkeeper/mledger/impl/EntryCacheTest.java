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

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.apache.bookkeeper.mledger.util.ManagedLedgerUtils.NO_MAX_SIZE_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BKException.BKNoSuchLedgerExistsException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.common.util.ThreadBoundExecutor;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.cache.EntryCache;
import org.apache.bookkeeper.mledger.impl.cache.EntryCacheManager;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EntryCacheTest extends MockedBookKeeperTestCase {

    private ManagedLedgerImpl ml;

    @Override
    protected void setUpTestCase() throws Exception {
        ml = mock(ManagedLedgerImpl.class);
        when(ml.getName()).thenReturn("name");
        when(ml.getExecutor()).thenReturn((ThreadBoundExecutor) bkExecutor.chooseThread());
        when(ml.getMbean()).thenReturn(new ManagedLedgerMBeanImpl(ml));
        when(ml.getConfig()).thenReturn(defaultConfig());
        when(ml.isBatchReadEnabled()).thenReturn(true);
        when(ml.getOptionalLedgerInfo(0L)).thenReturn(Optional.of(mock(
                ManagedLedgerInfo.LedgerInfo.class)));
    }

    @Test(timeOut = 5000)
    public void testRead() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 0; i < 10; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        final var entries = readEntry(entryCache, lh, 0, 9, () -> 0, null);
        assertEquals(entries.size(), 10);
        entries.forEach(Entry::release);

        // Verify no entries were read from bookkeeper
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
        verify(lh, never()).readAsync(anyLong(), anyLong());
    }

    @Test(timeOut = 5000)
    public void testReadMissingBefore() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 3; i < 10; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        final var entries = readEntry(entryCache, lh, 0, 9, () -> 0, null);
        assertEquals(entries.size(), 10);
    }

    @Test(timeOut = 5000)
    public void testReadMissingAfter() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        for (int i = 0; i < 8; i++) {
            entryCache.insert(EntryImpl.create(0, i, data));
        }

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        final var entries = readEntry(entryCache, lh, 0, 9, () -> 0, null);
        assertEquals(entries.size(), 10);
    }

    @Test(timeOut = 5000)
    public void testReadMissingMiddle() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 0, data));
        entryCache.insert(EntryImpl.create(0, 1, data));
        entryCache.insert(EntryImpl.create(0, 8, data));
        entryCache.insert(EntryImpl.create(0, 9, data));

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        final var entries = readEntry(entryCache, lh, 0, 9, () -> 0, null);
        assertEquals(entries.size(), 10);
    }

    @Test(timeOut = 5000)
    public void testReadMissingMultiple() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 0, data));
        entryCache.insert(EntryImpl.create(0, 2, data));
        entryCache.insert(EntryImpl.create(0, 5, data));
        entryCache.insert(EntryImpl.create(0, 8, data));

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        final var entries = readEntry(entryCache, lh, 0, 9, () -> 0, null);
        assertEquals(entries.size(), 10);
    }

    @Test
    public void testCachedReadReturnsDifferentByteBuffer() throws Exception {
        ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        readEntry(entryCache, lh, 0, 1, () -> 1, e -> {
            assertTrue(e instanceof ManagedLedgerException);
            assertTrue(e.getMessage().contains("LastConfirmedEntry is null when reading ledger 0"));
        });

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(-1, -1));
        readEntry(entryCache, lh, 0, 1, () -> 1, e -> {
            assertTrue(e instanceof ManagedLedgerException);
            assertTrue(e.getMessage().contains("LastConfirmedEntry is -1:-1 when reading ledger 0"));
        });

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 0));
        readEntry(entryCache, lh, 0, 1, () -> 1, e -> {
            assertTrue(e instanceof ManagedLedgerException);
            assertTrue(e.getMessage().contains("LastConfirmedEntry is 0:0 when reading entry 1"));
        });

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 1));
        List<Entry> cacheMissEntries = readEntry(entryCache, lh, 0, 1, () -> 1, null);
        // Ensure first entry is 0 and
        assertEquals(cacheMissEntries.size(), 2);
        assertEquals(cacheMissEntries.get(0).getEntryId(), 0);
        assertEquals(cacheMissEntries.get(0).getDataBuffer().readerIndex(), 0);

        // Move the reader index to simulate consumption
        cacheMissEntries.get(0).getDataBuffer().readerIndex(10);

        List<Entry> cacheHitEntries = readEntry(entryCache, lh, 0, 1, () -> 1, null);
        assertEquals(cacheHitEntries.get(0).getEntryId(), 0);
        assertEquals(cacheHitEntries.get(0).getDataBuffer().readerIndex(), 0);
    }

    @Test(timeOut = 5000)
    public void testReadWithError() throws Exception {
        final ReadHandle lh = getLedgerHandle();
        when(lh.getId()).thenReturn((long) 0);

        doAnswer((invocation) -> {
                CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
                future.completeExceptionally(new BKNoSuchLedgerExistsException());
                return future;
            }).when(lh).readUnconfirmedAsync(anyLong(), anyLong());

        EntryCacheManager cacheManager = factory.getEntryCacheManager();
        @Cleanup(value = "clear")
        EntryCache entryCache = cacheManager.getEntryCache(ml);

        byte[] data = new byte[10];
        entryCache.insert(EntryImpl.create(0, 2, data));

        when(ml.getLastConfirmedEntry()).thenReturn(PositionFactory.create(0, 9));
        readEntry(entryCache, lh, 0, 9, () -> 0, e ->
                assertTrue(e instanceof ManagedLedgerException.LedgerNotExistException));
    }

    @DataProvider
    public Object[][] unexpectedStorageEntries() {
        return new Object[][]{
                {0L, -1L}, // before the outer range
                {0L, 4L}, // after the outer range
                {0L, 1L}, // an already cached slot
                {0L, 0L}, // a duplicate storage entry
                {1L, 0L}, // a different ledger with an otherwise valid entry id
                {0L, 1L << 32} // an out-of-range id that wraps to a valid int index
        };
    }

    @Test(dataProvider = "unexpectedStorageEntries", timeOut = 30_000)
    public void testMixedReadDiscardsUnexpectedStorageEntries(long ledgerIdOffset, long unexpectedEntryId)
            throws Exception {
        MixedReadContext context = openLedgerWithPartialCache(false);
        ManagedLedgerImpl ledger = context.ledger();
        EntryImpl cached = readSingleEntry(ledger, context.cachedPosition());
        ByteBuf cachedBuffer = cached.getDataBuffer();
        EntryReadCountHandlerImpl cachedReadCount = (EntryReadCountHandlerImpl) cached.getReadCountHandler();
        int expectedCachedReads = cachedReadCount.getExpectedReadCount();
        List<List<Long>> reads = new CopyOnWriteArrayList<>();
        ByteBuf unexpectedBuffer = Unpooled.wrappedBuffer(new byte[]{99});
        List<Entry> result = null;
        try {
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                reads.add(List.of(first, last));
                if (first == 0 && last == 0) {
                    List<LedgerEntry> malformed = new ArrayList<>();
                    for (LedgerEntry entry : entries) {
                        malformed.add(((LedgerEntryImpl) entry).duplicate());
                    }
                    entries.close();
                    malformed.add(LedgerEntryImpl.create(ledgerId + ledgerIdOffset, unexpectedEntryId,
                            unexpectedBuffer.readableBytes(), unexpectedBuffer.retain()));
                    return CompletableFuture.completedFuture(LedgerEntriesImpl.create(malformed));
                }
                return CompletableFuture.completedFuture(entries);
            });

            result = readCursorEntries(context.cursor(), 4).get(10, TimeUnit.SECONDS);
            assertThat(reads).containsExactlyInAnyOrder(List.of(0L, 0L), List.of(2L, 3L));
            assertThat(result).hasSize(4);
            for (int i = 0; i < result.size(); i++) {
                assertThat(result.get(i).getPosition()).isEqualTo(
                        PositionFactory.create(context.cachedPosition().getLedgerId(), i));
                assertThat(result.get(i).getData()).containsExactly((byte) i);
            }
            assertThat(result.get(1).getReadCountHandler()).isSameAs(cachedReadCount);
            assertThat(cachedReadCount.getExpectedReadCount()).isEqualTo(expectedCachedReads);
            // Copying into the cache leaves only our test reference after the discarded source is released.
            assertThat(unexpectedBuffer.refCnt()).isEqualTo(1);
            if (ledgerIdOffset == 0 && unexpectedEntryId == 4) {
                EntryImpl unexpectedCached = readSingleEntry(ledger,
                        PositionFactory.create(context.cachedPosition().getLedgerId(), unexpectedEntryId));
                try {
                    assertThat(((EntryReadCountHandlerImpl) unexpectedCached.getReadCountHandler())
                            .getExpectedReadCount()).isEqualTo(2);
                } finally {
                    unexpectedCached.setDecreaseReadCountOnRelease(false);
                    unexpectedCached.release();
                }
            }

            result.forEach(Entry::release);
            result = null;
            assertThat(cachedReadCount.getExpectedReadCount()).isEqualTo(expectedCachedReads - 1);
        } finally {
            bkc.setReadHandleInterceptor(null);
            if (result != null) {
                result.forEach(Entry::release);
            }
            cached.setDecreaseReadCountOnRelease(false);
            cached.release();
            ledger.entryCache.clear();
            unexpectedBuffer.release();
            ledger.close();
        }
        // An overwritten cache copy would still retain this buffer after both the cache and probe are released.
        assertThat(cachedBuffer.refCnt()).isZero();
        assertThat(unexpectedBuffer.refCnt()).isZero();
    }

    @DataProvider
    public Object[][] mixedReadModes() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "mixedReadModes", timeOut = 30_000)
    public void testMixedReadRetryDoesNotConsumeExpectedReads(boolean batchReadEnabled) throws Exception {
        MixedReadContext context = openLedgerWithPartialCache(batchReadEnabled);
        ManagedLedgerImpl ledger = context.ledger();
        EntryImpl cached = readSingleEntry(ledger, context.cachedPosition());
        EntryReadCountHandlerImpl cachedReadCount = (EntryReadCountHandlerImpl) cached.getReadCountHandler();
        int expectedCachedReads = cachedReadCount.getExpectedReadCount();
        CompletableFuture<LedgerEntries> failedRead = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> failedReadEntries = new CompletableFuture<>();
        List<List<Long>> reads = new CopyOnWriteArrayList<>();
        EntryImpl successfulPartialRead = null;
        List<Entry> result = null;
        CompletableFuture<List<Entry>> read = null;
        try {
            bkc.setReadHandleInterceptor((ledgerId, first, last, entries) -> {
                reads.add(List.of(first, last));
                if (first == 2 && last == 3) {
                    failedReadEntries.complete(entries);
                    return failedRead;
                }
                return CompletableFuture.completedFuture(entries);
            });
            read = readCursorEntries(context.cursor(), 4);
            // Hold the failing gap until the other gap has reached the real storage-to-cache conversion.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ledger.entryCache.getSize()).isEqualTo(2));
            successfulPartialRead = readSingleEntry(ledger,
                    PositionFactory.create(context.cachedPosition().getLedgerId(), 0));
            EntryReadCountHandlerImpl partialReadCount =
                    (EntryReadCountHandlerImpl) successfulPartialRead.getReadCountHandler();
            assertThat(partialReadCount.getExpectedReadCount()).isEqualTo(2);
            failedReadEntries.get(10, TimeUnit.SECONDS).close();
            failedRead.completeExceptionally(new ManagedLedgerException("injected missing-range read failure"));

            result = read.get(10, TimeUnit.SECONDS);
            assertThat(reads).containsExactlyInAnyOrder(List.of(0L, 0L), List.of(2L, 3L), List.of(0L, 3L));
            assertThat(result).hasSize(4);
            for (int i = 0; i < result.size(); i++) {
                assertThat(result.get(i).getData()).containsExactly((byte) i);
            }
            assertThat(cachedReadCount.getExpectedReadCount()).isEqualTo(expectedCachedReads);
            assertThat(partialReadCount.getExpectedReadCount()).isEqualTo(2);
            result.forEach(Entry::release);
            result = null;
            // Only retry results were delivered. Discarding the first attempt must not mark its entries as read.
            assertThat(cachedReadCount.getExpectedReadCount()).isEqualTo(expectedCachedReads);
            assertThat(partialReadCount.getExpectedReadCount()).isEqualTo(2);
        } finally {
            bkc.setReadHandleInterceptor(null);
            if (read != null) {
                read.cancel(false);
            }
            failedReadEntries.thenAccept(entries -> {
                if (!failedRead.isDone()) {
                    entries.close();
                    failedRead.completeExceptionally(new ManagedLedgerException("test cleanup"));
                }
            });
            if (result != null) {
                result.forEach(Entry::release);
            }
            if (successfulPartialRead != null) {
                successfulPartialRead.setDecreaseReadCountOnRelease(false);
                successfulPartialRead.release();
            }
            cached.setDecreaseReadCountOnRelease(false);
            cached.release();
            ledger.close();
        }
    }

    private MixedReadContext openLedgerWithPartialCache(boolean batchReadEnabled) throws Exception {
        factory.getConfig().setCopyEntriesInCache(true);
        factory.getConfig().setCacheEvictionTimeThresholdMillis(TimeUnit.MINUTES.toMillis(1));
        ManagedLedgerConfig config = defaultConfig();
        config.setCacheEvictionByExpectedReadCount(true);
        config.setBatchReadEnabled(batchReadEnabled);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("mixed-read", config);
        ManagedCursor cursor = ledger.openCursor("reader");
        ledger.openCursor("other-reader");
        List<Position> positions = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            positions.add(ledger.addEntry(new byte[]{(byte) i}));
        }
        ledger.entryCache.clear();
        // Populate just entry 1 by reading through the cursor, then rewind to create gaps [0,0] and [2,3].
        cursor.seek(positions.get(1));
        List<Entry> warmup = readCursorEntries(cursor, 1).get(10, TimeUnit.SECONDS);
        warmup.forEach(Entry::release);
        cursor.seek(positions.get(0));
        assertThat(ledger.entryCache.getSize()).isEqualTo(1);
        return new MixedReadContext(ledger, cursor, positions.get(1));
    }

    private record MixedReadContext(ManagedLedgerImpl ledger, ManagedCursor cursor, Position cachedPosition) {
    }

    private static CompletableFuture<List<Entry>> readCursorEntries(ManagedCursor cursor, int count) {
        CompletableFuture<List<Entry>> result = new CompletableFuture<>();
        cursor.asyncReadEntries(count, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                if (!result.complete(entries)) {
                    entries.forEach(Entry::release);
                }
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null, PositionFactory.LATEST);
        return result;
    }

    private static EntryImpl readSingleEntry(ManagedLedgerImpl ledger, Position position) throws Exception {
        CompletableFuture<Entry> result = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                result.complete(entry);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                result.completeExceptionally(exception);
            }
        }, null);
        return (EntryImpl) result.get(10, TimeUnit.SECONDS);
    }

    static ReadHandle getLedgerHandle() {
        final ReadHandle lh = mock(ReadHandle.class);
        doAnswer((invocation) -> {
                Object[] args = invocation.getArguments();
                long firstEntry = (Long) args[0];
                long lastEntry = (Long) args[1];

                List<LedgerEntry> entries = new ArrayList<>();
                for (int i = 0; i <= (lastEntry - firstEntry); i++) {
                    entries.add(LedgerEntryImpl.create(0, i, 10, Unpooled.wrappedBuffer(new byte[10])));
                }
                LedgerEntries ledgerEntries = mock(LedgerEntries.class);
                doAnswer((invocation2) -> entries.iterator()).when(ledgerEntries).iterator();
                return CompletableFuture.completedFuture(ledgerEntries);
            }).when(lh).readUnconfirmedAsync(anyLong(), anyLong());
        // Batch reads use the ReadHandle default, which delegates to the stubbed readUnconfirmedAsync
        when(lh.batchReadUnconfirmedAsync(anyLong(), anyInt(), anyLong())).thenCallRealMethod();

        return lh;
    }

    private List<Entry> readEntry(EntryCache entryCache, ReadHandle lh, long firstEntry, long lastEntry,
                                  IntSupplier expectedReadCount, Consumer<Throwable> assertion)
            throws InterruptedException {
        final var future = new CompletableFuture<List<Entry>>();
        entryCache.asyncReadEntry(lh, firstEntry, lastEntry, NO_MAX_SIZE_LIMIT, expectedReadCount,
                new ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        future.complete(entries);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, null);
        try {
            final var entries = future.get();
            assertNull(assertion);
            return entries;
        } catch (ExecutionException e) {
            if (assertion != null) {
                assertion.accept(e.getCause());
            }
            return List.of();
        }
    }
}
