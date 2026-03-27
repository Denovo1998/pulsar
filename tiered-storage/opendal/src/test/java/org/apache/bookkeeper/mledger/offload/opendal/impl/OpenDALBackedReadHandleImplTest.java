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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffloadIndexEntryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.net.BookieId;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OpenDALBackedReadHandleImplTest {

    private final OffsetsCache offsetsCache = new OffsetsCache();
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    @AfterClass(alwaysRun = true)
    public void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        offsetsCache.close();
    }

    private String getExpectedEntryContent(int entryId) {
        return "Entry " + entryId;
    }

    private Pair<OpenDALBackedReadHandleImpl, ByteBuf> createReadHandle(
            long ledgerId, int entries, boolean hasDirtyData) throws Exception {
        List<Pair<Integer, Integer>> offsets = new ArrayList<>();
        int totalLen = 0;
        ByteBuf data = ByteBufAllocator.DEFAULT.heapBuffer(1024);
        data.writeInt(0);
        data.writerIndex(128);
        for (int i = 0; i < entries; i++) {
            if (hasDirtyData && i == 1) {
                data.writeBytes("dirty data".getBytes(UTF_8));
            }
            offsets.add(Pair.of(i, data.writerIndex()));
            offsetsCache.put(ledgerId, i, data.writerIndex());
            byte[] entryContent = getExpectedEntryContent(i).getBytes(UTF_8);
            totalLen += entryContent.length;
            data.writeInt(entryContent.length);
            data.writeLong(i);
            data.writeBytes(entryContent);
        }

        LedgerMetadata metadata = LedgerMetadataBuilder.create()
                .withId(ledgerId)
                .withEnsembleSize(1)
                .withWriteQuorumSize(1)
                .withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword("pwd".getBytes(UTF_8))
                .withClosedState()
                .withLastEntryId(entries)
                .withLength(totalLen)
                .newEnsembleEntry(0L, Arrays.asList(BookieId.parse("127.0.0.1:3181")))
                .build();

        BackedInputStreamImpl inputStream = new BackedInputStreamImpl(data);
        TestOffloadIndexBlock index = new TestOffloadIndexBlock(metadata);
        for (Pair<Integer, Integer> pair : offsets) {
            index.put(pair.getLeft(), OffloadIndexEntryImpl.of(pair.getLeft(), 0, pair.getRight(), 0));
        }
        return Pair.of(new OpenDALBackedReadHandleImpl(ledgerId, index, inputStream, executor, offsetsCache), data);
    }

    private static class TestOffloadIndexBlock implements OffloadIndexBlock {
        private final LedgerMetadata ledgerMetadata;
        private final java.util.NavigableMap<Long, OffloadIndexEntry> entries = new java.util.TreeMap<>();

        private TestOffloadIndexBlock(LedgerMetadata ledgerMetadata) {
            this.ledgerMetadata = ledgerMetadata;
        }

        private void put(long entryId, OffloadIndexEntry entry) {
            entries.put(entryId, entry);
        }

        @Override
        public OffloadIndexEntry getIndexEntryForEntry(long messageEntryId) throws IOException {
            return entries.floorEntry(messageEntryId).getValue();
        }

        @Override
        public int getEntryCount() {
            return entries.size();
        }

        @Override
        public LedgerMetadata getLedgerMetadata() {
            return ledgerMetadata;
        }

        @Override
        public long getDataObjectLength() {
            return 0;
        }

        @Override
        public long getDataBlockHeaderLength() {
            return 0;
        }

        @Override
        public IndexInputStream toStream() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // no-op
        }
    }

    private static class BackedInputStreamImpl extends BackedInputStream {

        private final ByteBuf data;

        private BackedInputStreamImpl(ByteBuf data) {
            this.data = data;
        }

        @Override
        public void seek(long position) {
            data.readerIndex((int) position);
        }

        @Override
        public void seekForward(long position) throws IOException {
            data.readerIndex((int) position);
        }

        @Override
        public long getCurrentPosition() {
            return data.readerIndex();
        }

        @Override
        public int read() throws IOException {
            if (data.readableBytes() == 0) {
                throw new EOFException("The input-stream has no bytes to read");
            }
            return data.readByte();
        }

        @Override
        public int available() throws IOException {
            return data.readableBytes();
        }
    }

    @DataProvider
    public Object[][] streamStartAt() {
        return new Object[][]{
                {0, false},
                {1, false},
                {128, false},
                {0, true},
                {1, true},
                {128, true}
        };
    }

    @Test(dataProvider = "streamStartAt")
    public void testRead(int streamStartAt, boolean hasDirtyData) throws Exception {
        int entryCount = 5;
        Pair<OpenDALBackedReadHandleImpl, ByteBuf> ledgerDataPair = createReadHandle(1, entryCount, hasDirtyData);
        OpenDALBackedReadHandleImpl ledger = ledgerDataPair.getLeft();
        ByteBuf data = ledgerDataPair.getRight();
        data.readerIndex(streamStartAt);

        for (int i = 0; i < entryCount; i++) {
            LedgerEntries entries = ledger.read(i, i);
            assertEquals(new String(entries.iterator().next().getEntryBytes()), getExpectedEntryContent(i));
        }

        LedgerEntries entries1 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator1 = entries1.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator1.next().getEntryBytes()), getExpectedEntryContent(i));
        }

        LedgerEntries entries2 = ledger.read(0, entryCount - 2);
        Iterator<LedgerEntry> iterator2 = entries2.iterator();
        for (int i = 0; i < entryCount - 1; i++) {
            assertEquals(new String(iterator2.next().getEntryBytes()), getExpectedEntryContent(i));
        }

        LedgerEntries entries3 = ledger.read(0, entryCount - 1);
        Iterator<LedgerEntry> iterator3 = entries3.iterator();
        for (int i = 0; i < entryCount; i++) {
            assertEquals(new String(iterator3.next().getEntryBytes()), getExpectedEntryContent(i));
        }

        ledger.close();
    }
}
