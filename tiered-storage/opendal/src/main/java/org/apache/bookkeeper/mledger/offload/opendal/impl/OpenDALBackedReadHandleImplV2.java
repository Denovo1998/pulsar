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

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedList;
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
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.offload.opendal.storage.OpenDALStorage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
public class OpenDALBackedReadHandleImplV2 implements ReadHandle, OffloadedLedgerHandle {

    private static final AtomicIntegerFieldUpdater<OpenDALBackedReadHandleImplV2> PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(OpenDALBackedReadHandleImplV2.class, "pendingRead");

    private final long ledgerId;
    private final List<OffloadIndexBlockV2> indices;
    private final List<BackedInputStream> inputStreams;
    private final List<DataInputStream> dataStreams;
    private final ExecutorService executor;
    private final AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();

    private volatile State state;
    private volatile int pendingRead;
    private volatile long lastAccessTimestamp = System.currentTimeMillis();

    enum State {
        Opened,
        Closed
    }

    static class GroupedReader {
        public final long ledgerId;
        public final long firstEntry;
        public final long lastEntry;
        OffloadIndexBlockV2 index;
        BackedInputStream inputStream;
        DataInputStream dataStream;

        GroupedReader(long ledgerId,
                      long firstEntry,
                      long lastEntry,
                      OffloadIndexBlockV2 index,
                      BackedInputStream inputStream,
                      DataInputStream dataStream) {
            this.ledgerId = ledgerId;
            this.firstEntry = firstEntry;
            this.lastEntry = lastEntry;
            this.index = index;
            this.inputStream = inputStream;
            this.dataStream = dataStream;
        }

        @Override
        public String toString() {
            return "GroupedReader{ledgerId=" + ledgerId + ", firstEntry=" + firstEntry + ", lastEntry=" + lastEntry
                    + '}';
        }
    }

    private OpenDALBackedReadHandleImplV2(long ledgerId,
                                          List<OffloadIndexBlockV2> indices,
                                          List<BackedInputStream> inputStreams,
                                          ExecutorService executor) {
        this.ledgerId = ledgerId;
        this.indices = indices;
        this.inputStreams = inputStreams;
        this.dataStreams = new LinkedList<>();
        for (BackedInputStream inputStream : inputStreams) {
            dataStreams.add(new DataInputStream(inputStream));
        }
        this.executor = executor;
        this.state = State.Opened;
    }

    @Override
    public long getId() {
        return ledgerId;
    }

    @Override
    public LedgerMetadata getLedgerMetadata() {
        // Return the most complete one.
        return indices.get(indices.size() - 1).getLedgerMetadata(ledgerId);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (closeFuture.get() != null || !closeFuture.compareAndSet(null, new CompletableFuture<>())) {
            return closeFuture.get();
        }

        CompletableFuture<Void> promise = closeFuture.get();
        executor.execute(() -> {
            try {
                for (OffloadIndexBlockV2 indexBlock : indices) {
                    indexBlock.close();
                }
                for (DataInputStream dataStream : dataStreams) {
                    dataStream.close();
                }
                state = State.Closed;
                promise.complete(null);
            } catch (IOException t) {
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<LedgerEntries> readAsync(long firstEntry, long lastEntry) {
        if (log.isDebugEnabled()) {
            log.debug("Ledger {}: reading {} - {}", getId(), firstEntry, lastEntry);
        }
        lastAccessTimestamp = System.currentTimeMillis();

        CompletableFuture<LedgerEntries> promise = new CompletableFuture<>();
        executor.execute(() -> {
            if (state == State.Closed) {
                log.warn("Reading a closed read handler. Ledger ID: {}, Read range: {}-{}",
                        ledgerId, firstEntry, lastEntry);
                promise.completeExceptionally(new ManagedLedgerException.OffloadReadHandleClosedException());
                return;
            }

            if (firstEntry > lastEntry
                    || firstEntry < 0
                    || lastEntry > getLastAddConfirmed()) {
                promise.completeExceptionally(new BKException.BKIncorrectParameterException());
                return;
            }

            List<LedgerEntry> entries = new ArrayList<>();
            List<GroupedReader> groupedReaders;
            try {
                groupedReaders = getGroupedReader(firstEntry, lastEntry);
            } catch (Exception e) {
                promise.completeExceptionally(e);
                return;
            }

            PENDING_READ_UPDATER.incrementAndGet(this);
            try {
                for (GroupedReader groupedReader : groupedReaders) {
                    long entriesToRead = (groupedReader.lastEntry - groupedReader.firstEntry) + 1;
                    long nextExpectedId = groupedReader.firstEntry;
                    while (entriesToRead > 0) {
                        int length = groupedReader.dataStream.readInt();
                        if (length < 0) { // hit padding or new block
                            groupedReader.inputStream.seek(groupedReader.index
                                    .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                    .getDataOffset());
                            continue;
                        }
                        long entryId = groupedReader.dataStream.readLong();

                        if (entryId == nextExpectedId) {
                            ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(length, length);
                            entries.add(LedgerEntryImpl.create(ledgerId, entryId, length, buf));
                            int toWrite = length;
                            while (toWrite > 0) {
                                toWrite -= buf.writeBytes(groupedReader.dataStream, toWrite);
                            }
                            entriesToRead--;
                            nextExpectedId++;
                        } else if (entryId > nextExpectedId) {
                            groupedReader.inputStream.seek(groupedReader.index
                                    .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                    .getDataOffset());
                        } else if (entryId < nextExpectedId
                                && !groupedReader.index.getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                .equals(groupedReader.index.getIndexEntryForEntry(groupedReader.ledgerId, entryId))) {
                            groupedReader.inputStream.seek(groupedReader.index
                                    .getIndexEntryForEntry(groupedReader.ledgerId, nextExpectedId)
                                    .getDataOffset());
                        } else if (entryId > groupedReader.lastEntry) {
                            log.info("Expected to read {}, but read {}, which is greater than last entry {}",
                                    nextExpectedId, entryId, groupedReader.lastEntry);
                            throw new BKException.BKUnexpectedConditionException();
                        } else {
                            skipFully(groupedReader.inputStream, length);
                        }
                    }
                }
                promise.complete(LedgerEntriesImpl.create(entries));
            } catch (Throwable t) {
                if (t instanceof FileNotFoundException) {
                    promise.completeExceptionally(new BKException.BKNoSuchLedgerExistsException());
                } else {
                    promise.completeExceptionally(t);
                }
                entries.forEach(LedgerEntry::close);
            } finally {
                PENDING_READ_UPDATER.decrementAndGet(this);
            }
        });
        return promise;
    }

    private List<GroupedReader> getGroupedReader(long firstEntry, long lastEntry) throws Exception {
        List<GroupedReader> groupedReaders = new LinkedList<>();
        for (int i = indices.size() - 1; i >= 0 && firstEntry <= lastEntry; i--) {
            OffloadIndexBlockV2 index = indices.get(i);
            long startEntryId = index.getStartEntryId(ledgerId);
            if (startEntryId > lastEntry) {
                log.debug("Entries are in earlier indices, skip this segment. ledgerId={}, beginEntryId={}",
                        ledgerId, startEntryId);
            } else {
                groupedReaders.add(new GroupedReader(ledgerId, startEntryId, lastEntry,
                        index, inputStreams.get(i), dataStreams.get(i)));
                lastEntry = startEntryId - 1;
            }
        }

        checkArgument(firstEntry > lastEntry);
        for (int i = 0; i < groupedReaders.size() - 1; i++) {
            GroupedReader readerI = groupedReaders.get(i);
            GroupedReader readerII = groupedReaders.get(i + 1);
            checkArgument(readerI.ledgerId == readerII.ledgerId);
            checkArgument(readerI.firstEntry >= readerII.lastEntry);
        }
        return groupedReaders;
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
                                  List<String> keys,
                                  List<String> indexKeys,
                                  long ledgerId,
                                  int readBufferSize,
                                  LedgerOffloaderStats offloaderStats,
                                  String managedLedgerName)
            throws IOException, BKException.BKNoSuchLedgerExistsException {
        List<BackedInputStream> inputStreams = new LinkedList<>();
        List<OffloadIndexBlockV2> indices = new LinkedList<>();
        String topicName = managedLedgerName != null
                ? TopicName.fromPersistenceNamingEncoding(managedLedgerName)
                : null;

        for (int i = 0; i < indexKeys.size(); i++) {
            String indexKey = indexKeys.get(i);
            String key = keys.get(i);

            long startTime = System.nanoTime();
            OpenDALStorage.ObjectMetadata meta;
            try {
                meta = storage.stat(indexKey);
            } catch (FileNotFoundException notFound) {
                log.error("{} not found while opening V2 offloaded ledger {}", indexKey, ledgerId);
                throw new BKException.BKNoSuchLedgerExistsException();
            }
            if (offloaderStats != null && topicName != null) {
                offloaderStats.recordReadOffloadIndexLatency(topicName,
                        System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
            }
            if (meta.getSize() <= 0) {
                throw new IOException("Index object is empty: " + indexKey);
            }

            OffloadIndexBlockV2Builder indexBuilder = OffloadIndexBlockV2Builder.create();
            OffloadIndexBlockV2 index;
            try (InputStream payloadStream = storage.readRange(indexKey, 0, meta.getSize() - 1)) {
                index = indexBuilder.fromStream(payloadStream);
            } catch (FileNotFoundException notFound) {
                log.error("{} not found while opening V2 offloaded ledger {}", indexKey, ledgerId);
                throw new BKException.BKNoSuchLedgerExistsException();
            }

            BackedInputStream inputStream = new OpenDALBackedInputStream(storage, key,
                    index.getDataObjectLength(), readBufferSize, offloaderStats, managedLedgerName);
            inputStreams.add(inputStream);
            indices.add(index);
        }

        return new OpenDALBackedReadHandleImplV2(ledgerId, indices, inputStreams, executor);
    }

    @Override
    public long lastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    @Override
    public int getPendingRead() {
        return PENDING_READ_UPDATER.get(this);
    }

    private static void skipFully(InputStream in, long bytes) throws IOException {
        long remaining = bytes;
        while (remaining > 0) {
            long skipped = in.skip(remaining);
            if (skipped > 0) {
                remaining -= skipped;
                continue;
            }
            if (in.read() < 0) {
                throw new IOException("Unexpected EOF while skipping " + bytes + " bytes");
            }
            remaining--;
        }
    }
}
