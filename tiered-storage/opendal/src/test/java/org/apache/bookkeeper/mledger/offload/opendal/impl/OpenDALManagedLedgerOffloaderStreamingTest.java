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

import static org.testng.Assert.assertEquals;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloadHandle;
import org.apache.bookkeeper.mledger.LedgerOffloaderStatsDisable;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALOperatorProvider;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALTieredStorageConfiguration;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OperatorCache;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadContext;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.OffloadSegment;
import org.testng.annotations.Test;

public class OpenDALManagedLedgerOffloaderStreamingTest {

    private static ManagedLedger createMockManagedLedger() {
        return (ManagedLedger) Proxy.newProxyInstance(
                ManagedLedger.class.getClassLoader(),
                new Class<?>[]{ManagedLedger.class},
                (proxy, method, args) -> {
                    if ("getLedgerInfo".equals(method.getName()) && args != null && args.length == 1) {
                        long ledgerId = (long) args[0];
                        LedgerInfo ledgerInfo = LedgerInfo.newBuilder()
                                .setLedgerId(ledgerId)
                                .setSize(100)
                                .setEntries(100)
                                .build();
                        return CompletableFuture.completedFuture(ledgerInfo);
                    }
                    if ("toString".equals(method.getName())) {
                        return "MockManagedLedger";
                    }
                    if ("getName".equals(method.getName())) {
                        return "MockManagedLedger";
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    @Test
    public void testStreamingOffloadWriteThenRead() throws Exception {
        Random random = new Random(0);
        List<byte[]> expected = new ArrayList<>();

        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(2)
                .name("opendal-offloader-test")
                .build();
        OffsetsCache offsetsCache = new OffsetsCache();
        OperatorCache operatorCache = new OperatorCache();
        try {
            HashMap<String, String> props = new HashMap<>();
            props.put(OpenDALTieredStorageConfiguration.BLOB_STORE_PROVIDER_KEY, "transient");
            props.put(OpenDALTieredStorageConfiguration.MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES, "1000");
            props.put("managedLedgerOffloadMinBlockSizeInBytes", "1024");

            OpenDALTieredStorageConfiguration config = OpenDALTieredStorageConfiguration.create(props);
            OpenDALOperatorProvider operatorProvider = new OpenDALOperatorProvider(config, operatorCache);

            OpenDALManagedLedgerOffloader offloader = OpenDALManagedLedgerOffloader.create(
                    config,
                    new HashMap<>(),
                    scheduler,
                    scheduler,
                    LedgerOffloaderStatsDisable.INSTANCE,
                    offsetsCache,
                    operatorProvider);

            ManagedLedger managedLedger = createMockManagedLedger();
            UUID uuid = UUID.randomUUID();
            long ledgerId = 0;

            OffloadHandle offloadHandle = offloader.streamingOffload(
                            managedLedger, uuid, ledgerId, 0, config.getOffloadDriverMetadata())
                    .get(5, TimeUnit.SECONDS);

            for (int i = 0; i < 10; i++) {
                byte[] data = new byte[100];
                random.nextBytes(data);
                expected.add(data.clone());
                EntryImpl entry = EntryImpl.create(ledgerId, i, data);
                try {
                    assertEquals(offloadHandle.offerEntry(entry), OffloadHandle.OfferEntryResult.SUCCESS);
                } finally {
                    entry.release();
                }
            }

            offloadHandle.close();
            LedgerOffloader.OffloadResult offloadResult = offloadHandle.getOffloadResultAsync()
                    .get(30, TimeUnit.SECONDS);
            assertEquals(offloadResult.endLedger, ledgerId);
            assertEquals(offloadResult.endEntry, 9);

            OffloadContext offloadContext = OffloadContext.newBuilder()
                    .addOffloadSegment(OffloadSegment.newBuilder()
                            .setUidMsb(uuid.getMostSignificantBits())
                            .setUidLsb(uuid.getLeastSignificantBits())
                            .setComplete(true)
                            .setEndEntryId(9)
                            .build())
                    .build();

            ReadHandle readHandle = offloader.readOffloaded(ledgerId, offloadContext, config.getOffloadDriverMetadata())
                    .get(5, TimeUnit.SECONDS);
            try {
                LedgerEntries ledgerEntries = readHandle.readAsync(0, 9).get(10, TimeUnit.SECONDS);
                try {
                    for (LedgerEntry ledgerEntry : ledgerEntries) {
                        assertEquals(ledgerEntry.getEntryBytes(), expected.get((int) ledgerEntry.getEntryId()));
                    }
                } finally {
                    ledgerEntries.close();
                }
            } finally {
                readHandle.close();
            }
        } finally {
            operatorCache.close();
            offsetsCache.close();
            scheduler.shutdownNow();
        }
    }
}
