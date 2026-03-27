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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloadHandle.OfferEntryResult;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.OffloadedLedgerMetadata;
import org.apache.bookkeeper.mledger.OffloadedLedgerMetadataConsumer;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.OffloadSegmentInfoImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock.IndexInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BlockAwareSegmentInputStreamImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.BufferedOffloadStream;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.StreamingDataBlockHeaderImpl;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALOperatorProvider;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALTieredStorageConfiguration;
import org.apache.bookkeeper.mledger.offload.opendal.storage.OpenDALStorage;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * Tiered Storage Offloader backed by OpenDAL.
 *
 * <p>This implementation keeps the same object key naming and on-disk formats as the existing jcloud implementation.
 */
@Slf4j
public class OpenDALManagedLedgerOffloader implements LedgerOffloader {

    private static final String MANAGED_LEDGER_NAME = "ManagedLedgerName";

    private final OrderedScheduler scheduler;
    private final OrderedScheduler readExecutor;
    private final OpenDALTieredStorageConfiguration config;
    private final OffloadPolicies policies;
    private final Map<String, String> userMetadata;
    private final OpenDALOperatorProvider operatorProvider;
    private final OffsetsCache entryOffsetsCache;
    private final LedgerOffloaderStats offloaderStats;

    private final AtomicLong bufferLength = new AtomicLong(0);
    private final AtomicLong segmentLength = new AtomicLong(0);
    private final long maxBufferLength;
    private final ConcurrentLinkedQueue<Entry> offloadBuffer = new ConcurrentLinkedQueue<>();
    private final Duration maxSegmentCloseTime;
    private final long minSegmentCloseTimeMillis;
    private final long segmentBeginTimeMillis;
    private final long maxSegmentLength;
    private final int streamingBlockSize;

    private CompletableFuture<OffloadResult> offloadResult;
    private volatile Position lastOfferedPosition = PositionFactory.LATEST;
    private OffloadIndexBlockV2Builder streamingIndexBuilder;
    private OffloadSegmentInfoImpl segmentInfo;
    private OpenDALStorage streamingStorage;
    private OutputStream streamingDataOut;
    private String streamingDataBlockKey;
    private String streamingDataIndexKey;

    public static OpenDALManagedLedgerOffloader create(OpenDALTieredStorageConfiguration config,
                                                       Map<String, String> userMetadata,
                                                       OrderedScheduler scheduler,
                                                       OrderedScheduler readExecutor,
                                                       LedgerOffloaderStats offloaderStats,
                                                       OffsetsCache entryOffsetsCache,
                                                       OpenDALOperatorProvider operatorProvider) throws IOException {
        config.validate();
        return new OpenDALManagedLedgerOffloader(config, userMetadata, scheduler, readExecutor, offloaderStats,
                entryOffsetsCache, operatorProvider);
    }

    OpenDALManagedLedgerOffloader(OpenDALTieredStorageConfiguration config,
                                  Map<String, String> userMetadata,
                                  OrderedScheduler scheduler,
                                  OrderedScheduler readExecutor,
                                  LedgerOffloaderStats offloaderStats,
                                  OffsetsCache entryOffsetsCache,
                                  OpenDALOperatorProvider operatorProvider) {
        this.scheduler = scheduler;
        this.readExecutor = readExecutor;
        this.userMetadata = userMetadata != null ? userMetadata : Collections.emptyMap();
        this.config = config;
        Properties properties = new Properties();
        properties.putAll(config.getConfigProperties());
        this.policies = OffloadPoliciesImpl.create(properties);
        this.entryOffsetsCache = entryOffsetsCache;
        this.operatorProvider = operatorProvider;
        this.offloaderStats = offloaderStats;
        this.streamingBlockSize = config.getMinBlockSizeInBytes();
        this.maxSegmentCloseTime = Duration.ofSeconds(config.getMaxSegmentTimeInSecond());
        this.maxSegmentLength = config.getMaxSegmentSizeInBytes();
        this.minSegmentCloseTimeMillis = Duration.ofSeconds(config.getMinSegmentTimeInSecond()).toMillis();
        this.maxBufferLength = Math.max(config.getWriteBufferSizeInBytes(), config.getMinBlockSizeInBytes());
        this.segmentBeginTimeMillis = System.currentTimeMillis();
        log.info("OpenDAL offloader created. driver={}, scheme={}, bucket={}, endpoint={}, region={}",
                config.getDriver(), config.getScheme(), config.getBucket(), config.getServiceEndpoint(),
                config.getRegion());
    }

    @Override
    public String getOffloadDriverName() {
        return config.getDriver();
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return config.getOffloadDriverMetadata();
    }

    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle, UUID uuid, Map<String, String> extraMetadata) {
        final String managedLedgerName = extraMetadata != null ? extraMetadata.get(MANAGED_LEDGER_NAME) : null;
        final String topicName = managedLedgerName != null
                ? TopicName.fromPersistenceNamingEncoding(managedLedgerName)
                : "unknown";

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).execute(() -> {
            OpenDALStorage storage = new OpenDALStorage(operatorProvider, config.getOffloadDriverMetadata());
            String dataKey = DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexKey = DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid);

            if (!readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }

            long dataObjectLength = 0;
            try {
                OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                        .withLedgerMetadata(readHandle.getLedgerMetadata())
                        .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());

                long startEntry = 0;
                int partId = 1;
                long entryBytesWritten = 0;
                try (OutputStream dataOut = storage.openOutputStream(dataKey)) {
                    while (startEntry <= readHandle.getLastAddConfirmed()) {
                        int blockSize = BlockAwareSegmentInputStreamImpl.calculateBlockSize(
                                config.getMaxBlockSizeInBytes(), readHandle, startEntry, entryBytesWritten);

                        try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                                readHandle, startEntry, blockSize, this.offloaderStats, managedLedgerName)) {
                            copyStream(blockStream, dataOut);
                            indexBuilder.addBlock(startEntry, partId, blockSize);

                            if (blockStream.getEndEntryId() != -1) {
                                startEntry = blockStream.getEndEntryId() + 1;
                            } else {
                                break;
                            }
                            entryBytesWritten += blockStream.getBlockEntryBytesCount();
                            partId++;
                            dataObjectLength += blockSize;
                            this.offloaderStats.recordOffloadBytes(topicName, blockStream.getBlockEntryBytesCount());
                        }
                    }
                }

                try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                     IndexInputStream indexStream = index.toStream()) {
                    byte[] indexBytes = readAllBytes(indexStream, indexStream.getStreamSize());
                    Map<String, String> objectMetadata = new HashMap<>(userMetadata);
                    objectMetadata.put("role", "index");
                    if (extraMetadata != null) {
                        objectMetadata.putAll(extraMetadata);
                    }
                    storage.writeBytes(indexKey, indexBytes, DataBlockUtils.withVersionInfo(objectMetadata));
                }

                promise.complete(null);
            } catch (Throwable t) {
                try {
                    storage.delete(dataKey);
                } catch (Throwable t2) {
                    log.warn("Failed to cleanup data object {}", dataKey, t2);
                }
                try {
                    storage.delete(indexKey);
                } catch (Throwable t2) {
                    log.warn("Failed to cleanup index object {}", indexKey, t2);
                }
                this.offloaderStats.recordWriteToStorageError(topicName);
                this.offloaderStats.recordOffloadError(topicName);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String dataKey = DataBlockUtils.dataBlockOffloadKey(ledgerId, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(ledgerId, uid);
        String managedLedgerName = offloadDriverMetadata != null
                ? offloadDriverMetadata.get(MANAGED_LEDGER_NAME)
                : null;

        readExecutor.chooseThread(ledgerId).execute(() -> {
            try {
                OpenDALStorage storage = new OpenDALStorage(operatorProvider,
                        offloadDriverMetadata != null ? offloadDriverMetadata : Collections.emptyMap());
                promise.complete(OpenDALBackedReadHandleImpl.open(readExecutor.chooseThread(ledgerId), storage,
                        dataKey, indexKey, ledgerId, config.getReadBufferSizeInBytes(), this.offloaderStats,
                        managedLedgerName, this.entryOffsetsCache));
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                promise.completeExceptionally(e);
            } catch (Throwable t) {
                log.error("Failed readOffloaded ledger {}", ledgerId, t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId,
                                                       MLDataFormats.OffloadContext ledgerContext,
                                                       Map<String, String> offloadDriverMetadata) {
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();

        List<String> keys = new LinkedList<>();
        List<String> indexKeys = new LinkedList<>();
        for (MLDataFormats.OffloadSegment seg : ledgerContext.getOffloadSegmentList()) {
            UUID uuid = new UUID(seg.getUidMsb(), seg.getUidLsb());
            keys.add(uuid.toString());
            indexKeys.add(DataBlockUtils.indexBlockOffloadKey(uuid));
        }

        String managedLedgerName = offloadDriverMetadata != null
                ? offloadDriverMetadata.get(MANAGED_LEDGER_NAME)
                : null;
        readExecutor.chooseThread(ledgerId).execute(() -> {
            try {
                OpenDALStorage storage = new OpenDALStorage(operatorProvider,
                        offloadDriverMetadata != null ? offloadDriverMetadata : Collections.emptyMap());
                promise.complete(OpenDALBackedReadHandleImplV2.open(readExecutor.chooseThread(ledgerId), storage,
                        keys, indexKeys, ledgerId, config.getReadBufferSizeInBytes(), this.offloaderStats,
                        managedLedgerName));
            } catch (BKException.BKNoSuchLedgerExistsException e) {
                promise.completeExceptionally(e);
            } catch (Throwable t) {
                log.error("Failed readOffloaded (V2) ledger {}", ledgerId, t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<OffloadHandle> streamingOffload(ManagedLedger ml,
                                                             UUID uuid,
                                                             long beginLedger,
                                                             long beginEntry,
                                                             Map<String, String> driverMetadata) {
        if (this.segmentInfo != null) {
            CompletableFuture<OffloadHandle> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException("streamingOffload should only be called once"));
            return result;
        }

        this.segmentInfo = new OffloadSegmentInfoImpl(uuid, beginLedger, beginEntry, config.getDriver(),
                driverMetadata != null ? driverMetadata : Collections.emptyMap());
        this.offloadResult = new CompletableFuture<>();

        this.streamingIndexBuilder = OffloadIndexBlockV2Builder.create()
                .withDataBlockHeaderLength(StreamingDataBlockHeaderImpl.getDataStartOffset());
        this.streamingDataBlockKey = segmentInfo.uuid.toString();
        this.streamingDataIndexKey = DataBlockUtils.indexBlockOffloadKey(segmentInfo.uuid);
        this.streamingStorage = new OpenDALStorage(operatorProvider, config.getOffloadDriverMetadata());

        try {
            this.streamingDataOut = streamingStorage.openOutputStream(streamingDataBlockKey);
        } catch (IOException e) {
            CompletableFuture<OffloadHandle> result = new CompletableFuture<>();
            result.completeExceptionally(e);
            return result;
        }

        scheduler.chooseThread(segmentInfo).execute(() -> {
            log.info("Start streaming offload segment: {}", segmentInfo);
            streamingOffloadLoop(ml, 1, 0);
        });
        scheduler.schedule(this::closeSegment, maxSegmentCloseTime.toMillis(), TimeUnit.MILLISECONDS);

        return CompletableFuture.completedFuture(new OffloadHandle() {
            @Override
            public Position lastOffered() {
                return OpenDALManagedLedgerOffloader.this.lastOffered();
            }

            @Override
            public CompletableFuture<Position> lastOfferedAsync() {
                return CompletableFuture.completedFuture(lastOffered());
            }

            @Override
            public OfferEntryResult offerEntry(Entry entry) {
                return OpenDALManagedLedgerOffloader.this.offerEntry(entry);
            }

            @Override
            public CompletableFuture<OfferEntryResult> offerEntryAsync(Entry entry) {
                return CompletableFuture.completedFuture(offerEntry(entry));
            }

            @Override
            public CompletableFuture<OffloadResult> getOffloadResultAsync() {
                return OpenDALManagedLedgerOffloader.this.getOffloadResultAsync();
            }

            @Override
            public boolean close() {
                return OpenDALManagedLedgerOffloader.this.closeSegment();
            }
        });
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid, Map<String, String> offloadDriverMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        String dataKey = DataBlockUtils.dataBlockOffloadKey(ledgerId, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(ledgerId, uid);
        final String managedLedgerName =
                offloadDriverMetadata != null ? offloadDriverMetadata.get(MANAGED_LEDGER_NAME) : null;
        final String topicName = managedLedgerName != null
                ? TopicName.fromPersistenceNamingEncoding(managedLedgerName)
                : "unknown";
        scheduler.execute(() -> {
            try {
                OpenDALStorage storage = new OpenDALStorage(operatorProvider,
                        offloadDriverMetadata != null ? offloadDriverMetadata : Collections.emptyMap());
                storage.delete(dataKey);
                storage.delete(indexKey);
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete offloaded objects for ledger {}", ledgerId, t);
                promise.completeExceptionally(t);
            }
        });
        return promise.whenComplete((__, t) -> this.offloaderStats.recordDeleteOffloadOps(topicName, t == null));
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(UUID uid, Map<String, String> offloadDriverMetadata) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        String dataKey = uid.toString();
        String indexKey = DataBlockUtils.indexBlockOffloadKey(uid);
        scheduler.execute(() -> {
            try {
                OpenDALStorage storage = new OpenDALStorage(operatorProvider,
                        offloadDriverMetadata != null ? offloadDriverMetadata : Collections.emptyMap());
                storage.delete(dataKey);
                storage.delete(indexKey);
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete offloaded objects for uuid {}", uid, t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        return policies;
    }

    @Override
    public void close() {
        // All shared resources are managed by the factory (e.g. OperatorCache, OffsetsCache).
    }

    @Override
    public void scanLedgers(OffloadedLedgerMetadataConsumer consumer,
                            Map<String, String> offloadDriverMetadata) throws ManagedLedgerException {
        int batchSize = 100;
        String marker = null;
        OpenDALStorage storage = new OpenDALStorage(operatorProvider,
                offloadDriverMetadata != null ? offloadDriverMetadata : Collections.emptyMap());
        do {
            OpenDALStorage.ListResult list;
            try {
                list = storage.list("", marker, batchSize);
            } catch (IOException e) {
                throw ManagedLedgerException.getManagedLedgerException(e);
            }
            for (OpenDALStorage.Item item : list.getItems()) {
                String name = item.getPath();
                Long ledgerId = DataBlockUtils.parseLedgerId(name);
                if (ledgerId == null) {
                    continue;
                }
                String contextUuid = DataBlockUtils.parseContextUuid(name, ledgerId);
                Instant lastModified = item.getMetadata().getLastModified();
                long lastModifiedMillis = lastModified != null ? lastModified.toEpochMilli() : 0;
                OffloadedLedgerMetadata offloadedLedgerMetadata = OffloadedLedgerMetadata.builder()
                        .name(name)
                        .bucketName(config.getBucket())
                        .uuid(contextUuid)
                        .ledgerId(ledgerId)
                        .lastModified(lastModifiedMillis)
                        .size(item.getMetadata().getSize())
                        .uri(null)
                        .userMetadata(Collections.emptyMap())
                        .build();
                try {
                    boolean canContinue = consumer.accept(offloadedLedgerMetadata);
                    if (!canContinue) {
                        log.info("Iteration stopped by the OffloadedLedgerMetadataConsumer");
                        return;
                    }
                } catch (Exception err) {
                    if (err instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw ManagedLedgerException.getManagedLedgerException(err);
                }
            }
            marker = list.getNextMarker();
        } while (marker != null);
    }

    private void streamingOffloadLoop(ManagedLedger ml, int partId, long dataObjectLength) {
        if (offloadResult != null && offloadResult.isDone()) {
            return;
        }
        if (segmentInfo == null) {
            return;
        }
        if (segmentInfo.isClosed() && offloadBuffer.isEmpty()) {
            buildIndexAndCompleteResult(dataObjectLength);
            return;
        }

        if ((segmentInfo.isClosed() && !offloadBuffer.isEmpty()) || bufferLength.get() >= streamingBlockSize) {
            List<Entry> entries = new LinkedList<>();
            int blockEntrySize = 0;

            Entry firstEntry = offloadBuffer.poll();
            if (firstEntry == null) {
                scheduler.chooseThread(segmentInfo).schedule(() -> streamingOffloadLoop(ml, partId, dataObjectLength),
                        100, TimeUnit.MILLISECONDS);
                return;
            }
            int firstEntrySize = firstEntry.getLength();
            bufferLength.addAndGet(-firstEntrySize);
            blockEntrySize += firstEntrySize;
            entries.add(firstEntry);
            long blockLedgerId = firstEntry.getLedgerId();
            long blockEntryId = firstEntry.getEntryId();

            while (!offloadBuffer.isEmpty()
                    && offloadBuffer.peek().getLedgerId() == blockLedgerId
                    && blockEntrySize <= streamingBlockSize) {
                Entry entryInBlock = offloadBuffer.poll();
                if (entryInBlock == null) {
                    break;
                }
                int entrySize = entryInBlock.getLength();
                bufferLength.addAndGet(-entrySize);
                blockEntrySize += entrySize;
                entries.add(entryInBlock);
            }

            int blockSize = BufferedOffloadStream.calculateBlockSize(
                    streamingBlockSize, entries.size(), blockEntrySize);
            buildBlockAndWrite(ml, blockSize, entries, blockLedgerId, blockEntryId, partId);
            streamingOffloadLoop(ml, partId + 1, dataObjectLength + blockSize);
        } else {
            scheduler.chooseThread(segmentInfo).schedule(() -> streamingOffloadLoop(ml, partId, dataObjectLength),
                    100, TimeUnit.MILLISECONDS);
        }
    }

    private void buildBlockAndWrite(ManagedLedger ml,
                                    int blockSize,
                                    List<Entry> entries,
                                    long blockLedgerId,
                                    long beginEntryId,
                                    int partId) {
        try (BufferedOffloadStream payloadStream = new BufferedOffloadStream(blockSize, entries,
                blockLedgerId, beginEntryId)) {
            copyStream(payloadStream, streamingDataOut);
            streamingIndexBuilder.withDataBlockHeaderLength(StreamingDataBlockHeaderImpl.getDataStartOffset());
            streamingIndexBuilder.addBlock(blockLedgerId, beginEntryId, partId, blockSize);

            MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ml.getLedgerInfo(blockLedgerId).get();
            MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder ledgerInfoBuilder =
                    MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder();
            if (ledgerInfo != null) {
                ledgerInfoBuilder.mergeFrom(ledgerInfo);
            }
            if (ledgerInfoBuilder.getEntries() == 0) {
                ledgerInfoBuilder.setEntries(payloadStream.getEndEntryId() + 1);
            }
            streamingIndexBuilder.addLedgerMeta(blockLedgerId, ledgerInfoBuilder.build());
        } catch (Throwable e) {
            failStreamingOffload(e);
        }
    }

    private void buildIndexAndCompleteResult(long dataObjectLength) {
        try {
            if (streamingDataOut != null) {
                streamingDataOut.close();
                streamingDataOut = null;
            }

            streamingIndexBuilder.withDataObjectLength(dataObjectLength);
            OffloadIndexBlockV2 index = streamingIndexBuilder.buildV2();
            try (IndexInputStream indexStream = index.toStream()) {
                byte[] indexBytes = readAllBytes(indexStream, indexStream.getStreamSize());
                streamingStorage.writeBytes(streamingDataIndexKey, indexBytes,
                        DataBlockUtils.withVersionInfo(userMetadata));
            }

            offloadResult.complete(segmentInfo.result());
            log.info("Streaming offload segment completed {}", segmentInfo.result());
        } catch (Throwable t) {
            failStreamingOffload(t);
        }
    }

    private void failStreamingOffload(Throwable error) {
        if (offloadResult != null && offloadResult.isDone()) {
            return;
        }
        try {
            if (streamingDataOut != null) {
                streamingDataOut.close();
                streamingDataOut = null;
            }
        } catch (Throwable t) {
            log.warn("Failed to close streaming data output stream", t);
        }
        try {
            if (streamingStorage != null && streamingDataBlockKey != null) {
                streamingStorage.delete(streamingDataBlockKey);
            }
        } catch (Throwable t) {
            log.warn("Failed to cleanup streaming data object {}", streamingDataBlockKey, t);
        }
        try {
            if (streamingStorage != null && streamingDataIndexKey != null) {
                streamingStorage.delete(streamingDataIndexKey);
            }
        } catch (Throwable t) {
            log.warn("Failed to cleanup streaming index object {}", streamingDataIndexKey, t);
        }

        offloadResult.completeExceptionally(error);
    }

    private CompletableFuture<OffloadResult> getOffloadResultAsync() {
        return this.offloadResult;
    }

    private synchronized OfferEntryResult offerEntry(Entry entry) {
        if (segmentInfo == null) {
            return OfferEntryResult.FAIL_SEGMENT_CLOSED;
        }
        if (segmentInfo.isClosed()) {
            return OfferEntryResult.FAIL_SEGMENT_CLOSED;
        }
        if (maxBufferLength <= bufferLength.get()) {
            return OfferEntryResult.FAIL_BUFFER_FULL;
        }

        EntryImpl entryImpl = EntryImpl.create(entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer());
        offloadBuffer.add(entryImpl);
        bufferLength.getAndAdd(entryImpl.getLength());
        segmentLength.getAndAdd(entryImpl.getLength());
        lastOfferedPosition = entryImpl.getPosition();
        if (segmentLength.get() >= maxSegmentLength
                && System.currentTimeMillis() - segmentBeginTimeMillis >= minSegmentCloseTimeMillis) {
            closeSegment();
        }
        return OfferEntryResult.SUCCESS;
    }

    private synchronized boolean closeSegment() {
        if (segmentInfo == null) {
            return false;
        }
        boolean result = !segmentInfo.isClosed();
        if (result) {
            segmentInfo.closeSegment(lastOfferedPosition.getLedgerId(), lastOfferedPosition.getEntryId());
        }
        return result;
    }

    private Position lastOffered() {
        return lastOfferedPosition;
    }

    private static void copyStream(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[1024 * 64];
        int read;
        while ((read = in.read(buffer)) >= 0) {
            if (read == 0) {
                continue;
            }
            out.write(buffer, 0, read);
        }
    }

    private static byte[] readAllBytes(InputStream in, long size) throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IOException("Stream too large: " + size);
        }
        byte[] out = new byte[(int) size];
        int offset = 0;
        while (offset < out.length) {
            int read = in.read(out, offset, out.length - offset);
            if (read < 0) {
                break;
            }
            offset += read;
        }
        if (offset != out.length) {
            throw new IOException("Unexpected EOF: expected " + out.length + " bytes, got " + offset);
        }
        return out;
    }
}
