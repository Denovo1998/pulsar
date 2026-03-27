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
package org.apache.bookkeeper.mledger.offload.opendal.impl;

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LastConfirmedAndEntry;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadedLedgerHandle;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.mledger.offload.opendal.storage.OpenDALStorage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class OpenDALBackedReadHandleImpl implements ReadHandle, OffloadedLedgerHandle {

    protected static final AtomicIntegerFieldUpdater<OpenDALBackedReadHandleImpl> PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(OpenDALBackedReadHandleImpl.class, "pendingRead");

    private final long ledgerId;
    private final OffloadIndexBlock index;
    private final BackedInputStream inputStream;
    private final DataInputStream dataStream;
    private final ExecutorService executor;
    private final OffsetsCache entryOffsetsCache;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    enum State {
        Opened,
        Closed
    }

    private volatile State state;
    private volatile int pendingRead;
    private volatile long lastAccessTimestamp = System.currentTimeMillis();

    @VisibleForTesting
    OpenDALBackedReadHandleImpl(long ledgerId,
                                OffloadIndexBlock index,
                                BackedInputStream inputStream,
                                ExecutorService executor,
                                OffsetsCache entryOffsetsCache) {
        this.ledgerId = ledgerId;
        this.index = index;
        this.inputStream = inputStream;
        this.dataStream = new DataInputStream(inputStream);
        this.executor = executor;
        this.entryOffsetsCache = entryOffsetsCache;
        state = State.Opened;
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        return index.getLedgerMetadata();
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }

        CompletableFuture<Void> promise = closeFuture.get();
        executor.execute(() -> {
            try {
                index.close();
                inputStream.close();
                state = State.Closed;
                promise.complete(null);
            } catch (IOException t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    private class ReadTask implements Runnable {
        private final long firstEntry;
        private final long lastEntry;
        private final CompletableFuture<LedgerEntries> promise;
        private int seekedAndTryTimes = 0;

        ReadTask(long firstEntry, long lastEntry, CompletableFuture<LedgerEntries> promise) {
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.promise = promise;
        }

        @Override
        public void run() {
            if (state == State.Closed) {
                log.warn("Reading a closed read handler. Ledger ID: {}, Read range: {}-{}",
                        ledgerId, firstEntry, lastEntry);
                promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                return;
            }

            List<LedgerEntry> entryCollector = new ArrayList<>();
            try {
                if (firstEntry > lastEntry || firstEntry < 0 || lastEntry > getLastAddConfirmed()) {
                    promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                    return;
                }
                long entriesToRead = (lastEntry - firstEntry) + 1;
                long expectedEntryId = firstEntry;
                seekToEntryOffset(firstEntry);
                seekedAndTryTimes++;

                while (entriesToRead > 0) {
                    long currentPosition = inputStream.getCurrentPosition();
                    int length = dataStream.readInt();
                    if (length < 0) { // hit padding or new block
                        seekToEntryOffset(expectedEntryId);
                        continue;
                    }
                    long entryId = dataStream.readLong();
                    if (entryId == expectedEntryId) {
                        entryOffsetsCache.put(ledgerId, entryId, currentPosition);
                        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                        entryCollector.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                        int toWrite = length;
                        while (toWrite > 0) {
                            toWrite -= buf.writeBytes(dataStream, toWrite);
                        }
                        entriesToRead--;
                        expectedEntryId++;
                    } else {
                        handleUnexpectedEntryId(expectedEntryId, entryId);
                    }
                }
                promise.complete(LedgerEntriesImpl.create(entryCollector));
            } catch (Throwable t) {
                log.error("Failed to read entries {} - {} from the offloader in ledger {}, current position of input"
                        + " stream is {}", firstEntry, lastEntry, ledgerId, inputStream.getCurrentPosition(), t);
                if (t instanceof FileNotFoundException) {
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                } else {
                    promise.completeExceptionally(t);
                }
                entryCollector.forEach(LedgerEntry::close);
            }
        }

        private void handleUnexpectedEntryId(long expectedId, long actEntryId) throws Exception {
            LedgerMetadata ledgerMetadata = getLedgerMetadata();
            OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedId);
            OffloadIndexEntry offsetOfActId = actEntryId <= getLedgerMetadata().getLastEntryId() && actEntryId >= 0
                    ? index.getIndexEntryForEntry(actEntryId) : null;
            String logLine = String.format("Failed to read [ %s ~ %s ] of the ledger %s."
                            + " Because got a incorrect entry id %s, the offset is %s."
                            + " The expected entry id is %s, the offset is %s."
                            + " Have seeked and retry read times: %s. LAC is %s.",
                    firstEntry, lastEntry, ledgerId,
                    actEntryId, offsetOfActId == null ? "null because it does not exist"
                            : String.valueOf(offsetOfActId),
                    expectedId, String.valueOf(offsetOfExpectedId),
                    seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
            long maxTryTimes = Math.max(3, (lastEntry - firstEntry + 1) >> 2);
            if (seekedAndTryTimes > maxTryTimes) {
                log.error(logLine);
                throw new BKException.BKUnexpectedConditionException();
            } else {
                log.warn(logLine);
            }
            seekToEntryOffset(expectedId);
            seekedAndTryTimes++;
        }

        private void skipPreviousEntry(long startEntryId, long expectedEntryId) throws IOException, BKException {
            long nextExpectedEntryId = startEntryId;
            while (nextExpectedEntryId < expectedEntryId) {
                long offset = inputStream.getCurrentPosition();
                int len = dataStream.readInt();
                if (len < 0) {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                                    + " Because failed to skip a previous entry {}, len: {}, got a negative len."
                                    + " The expected entry id is {}, the offset is {}."
                                    + " Have seeked and retry read times: {}. LAC is {}.",
                            firstEntry, lastEntry, ledgerId,
                            nextExpectedEntryId, len,
                            expectedEntryId, String.valueOf(offsetOfExpectedId),
                            seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                    throw new BKException.BKUnexpectedConditionException();
                }
                long entryId = dataStream.readLong();
                if (entryId == nextExpectedEntryId) {
                    long skipped = inputStream.skip(len);
                    if (skipped != len) {
                        LedgerMetadata ledgerMetadata = getLedgerMetadata();
                        OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                        log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                                        + " Because failed to skip a previous entry {}, offset: {}, len: {},"
                                        + " there is no more data."
                                        + " The expected entry id is {}, the offset is {}."
                                        + " Have seeked and retry read times: {}. LAC is {}.",
                                firstEntry, lastEntry, ledgerId,
                                entryId, offset, len,
                                expectedEntryId, String.valueOf(offsetOfExpectedId),
                                seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId()
                                        : "unknown");
                        throw new BKException.BKUnexpectedConditionException();
                    }
                    nextExpectedEntryId++;
                } else {
                    LedgerMetadata ledgerMetadata = getLedgerMetadata();
                    OffloadIndexEntry offsetOfExpectedId = index.getIndexEntryForEntry(expectedEntryId);
                    log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                                    + " Because got a incorrect entry id {},."
                                    + " The expected entry id is {}, the offset is {}."
                                    + " Have seeked and retry read times: {}. LAC is {}.",
                            firstEntry, lastEntry, ledgerId,
                            entryId, expectedEntryId, String.valueOf(offsetOfExpectedId),
                            seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                    throw new BKException.BKUnexpectedConditionException();
                }
            }
        }

        private void seekToEntryOffset(long expectedEntryId) throws IOException, BKException {
            Long cachedPreciseIndex = entryOffsetsCache.getIfPresent(ledgerId, expectedEntryId);
            if (cachedPreciseIndex != null) {
                inputStream.seek(cachedPreciseIndex);
                return;
            }

            OffloadIndexEntry indexOfNearestEntry = index.getIndexEntryForEntry(expectedEntryId);
            if (indexOfNearestEntry.getEntryId() == expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                return;
            }

            Long cachedPreviousKnownOffset = entryOffsetsCache.getIfPresent(ledgerId, expectedEntryId - 1);
            if (cachedPreviousKnownOffset != null) {
                inputStream.seek(cachedPreviousKnownOffset);
                skipPreviousEntry(expectedEntryId - 1, expectedEntryId);
                return;
            }

            if (indexOfNearestEntry.getEntryId() < expectedEntryId) {
                inputStream.seek(indexOfNearestEntry.getDataOffset());
                skipPreviousEntry(indexOfNearestEntry.getEntryId(), expectedEntryId);
            } else {
                LedgerMetadata ledgerMetadata = getLedgerMetadata();
                log.error("Failed to read [ {} ~ {} ] of the ledger {}."
                                + " Because got a incorrect index {} of the entry {}, which is greater than expected."
                                + " Have seeked and retry read times: {}. LAC is {}.",
                        firstEntry, lastEntry, ledgerId,
                        String.valueOf(indexOfNearestEntry), expectedEntryId,
                        seekedAndTryTimes, ledgerMetadata != null ? ledgerMetadata.getLastEntryId() : "unknown");
                throw new BKException.BKUnexpectedConditionException();
            }
        }
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (log.isDebugEnabled()) {
            log.debug("Ledger {}: reading {} - {} ({} entries}",
                    getId(), firstEntry, lastEntry, (1 + lastEntry - firstEntry));
        }
        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();

        PENDING_READ_UPDATER.incrementAndGet(this);
        promise.whenComplete((__, ex) -> {
            lastAccessTimestamp = System.currentTimeMillis();
            PENDING_READ_UPDATER.decrementAndGet(OpenDALBackedReadHandleImpl.this);
        });
        executor.execute(new ReadTask(firstEntry, lastEntry, promise));
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readUnconfirmedAsync(long firstEntry, long lastEntry) {
        return readAsync(firstEntry, lastEntry);
    }

    @Override
    public CompletableFuture<Long> readLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public CompletableFuture<Long> tryReadLastAddConfirmedAsync() {
        return CompletableFuture.completedFuture(getLastAddConfirmed());
    }

    @Override
    public long getLastAddConfirmed() {
        return getLedgerMetadata().getLastEntryId();
    }

    @Override
    public long getLength() {
        return getLedgerMetadata().getLength();
    }

    @Override
    public boolean isClosed() {
        return getLedgerMetadata().isClosed();
    }

    @Override
    public CompletableFuture<LastConfirmedAndEntry> readLastAddConfirmedAndEntryAsync(long entryId,
                                                                                      long timeOutInMillis,
                                                                                      boolean parallel) {
        CompletableFuture<LastConfirmedAndEntry> promise = new CompletableFuture<>();
        promise.completeExceptionally(new UnsupportedOperationException());
        return promise;
    }

    public static ReadHandle open(ScheduledExecutorService executor,
                                  OpenDALStorage storage,
                                  String dataKey,
                                  String indexKey,
                                  long ledgerId,
                                  int readBufferSize,
                                  LedgerOffloaderStats offloaderStats,
                                  String managedLedgerName,
                                  OffsetsCache entryOffsetsCache)
            throws IOException, BKException.BKNoSuchLedgerExistsException {
        int retryCount = 3;
        OffloadIndexBlock index = null;
        IOException lastException = null;
        String topicName = TopicName.fromPersistenceNamingEncoding(managedLedgerName);
        while (retryCount-- > 0) {
            long readIndexStartTime = System.nanoTime();
            try {
                OpenDALStorage.ObjectMetadata meta = storage.stat(indexKey);
                if (meta.getSize() <= 0) {
                    throw new IOException("Index object is empty: " + indexKey);
                }
                try (InputStream in = storage.readRange(indexKey, 0, meta.getSize() - 1)) {
                    OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create();
                    index = (OffloadIndexBlock) indexBuilder.fromStream(in);
                }
                offloaderStats.recordReadOffloadIndexLatency(topicName,
                        System.nanoTime() - readIndexStartTime, TimeUnit.NANOSECONDS);
                lastException = null;
                break;
            } catch (FileNotFoundException notFound) {
                log.error("{} not found for ledger {}", indexKey, ledgerId);
                throw new BKException.BKNoSuchLedgerExistsException();
            } catch (IOException e) {
                log.warn("Failed to get index block from the offloaded index file {}, still have {} times to retry",
                        indexKey, retryCount, e);
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
        if (index == null) {
            throw new IOException("Failed to open offloaded index " + indexKey);
        }

        BackedInputStream inputStream = new OpenDALBackedInputStream(storage, dataKey,
                index.getDataObjectLength(), readBufferSize, offloaderStats, managedLedgerName);
        return new OpenDALBackedReadHandleImpl(ledgerId, index, inputStream, executor, entryOffsetsCache);
    }

    @VisibleForTesting
    State getState() {
        return this.state;
    }

    @Override
    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    @Override
    public int getPendingRead() {
        return PENDING_READ_UPDATER.get(this);
    }
}
