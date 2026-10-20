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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.ReadOnlyManagedLedger;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ReadOnlyManagedLedgerImplTest extends MockedBookKeeperTestCase {
    private static final String MANAGED_LEDGER_NAME_NON_PROPERTIES = "ml-non-properties";
    private static final String MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES = "ml-attached-properties";
    private final List<ReadHandle> openedHandles = new CopyOnWriteArrayList<>();

    @Override
    @SuppressWarnings("unchecked")
    protected void startBookKeeper() throws Exception {
        super.startBookKeeper();
        openedHandles.clear();
        bkc = spy(bkc);
        doAnswer(invocation -> {
            OpenBuilder builder = (OpenBuilder) invocation.callRealMethod();
            OpenBuilder trackedBuilder = spy(builder);
            doAnswer(openInvocation -> {
                CompletableFuture<ReadHandle> opening =
                        (CompletableFuture<ReadHandle>) openInvocation.callRealMethod();
                return opening.thenApply(handle -> {
                    ReadHandle trackedHandle = spy(handle);
                    openedHandles.add(trackedHandle);
                    return trackedHandle;
                });
            }).when(trackedBuilder).execute();
            return trackedBuilder;
        }).when(bkc).newOpenLedgerOp();
    }

    @Override
    protected void cleanUpTestCase() throws Exception {
        for (ReadHandle handle : openedHandles) {
            handle.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @DataProvider(name = "cacheEnabled")
    public Object[][] cacheEnabled() {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "cacheEnabled")
    public void testReadFailureInvalidatesReadOnlyHandle(boolean cacheEnabled) throws Exception {
        configureCache(cacheEnabled);
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("readonly-read-failure", config);
        Position position = writer.addEntry(new byte[] {1});
        writer.close();
        ReadOnlyManagedLedger readOnly = openReadOnly(writer.getName(), config);
        ReadOnlyCursorImpl cursor = (ReadOnlyCursorImpl) readOnly.createReadOnlyCursor(PositionFactory.EARLIEST);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
        try {
            readEntry(readOnly, position).get(5, TimeUnit.SECONDS);
            ReadHandle failedHandle = ledger.ledgerCache.get(position.getLedgerId()).get(5, TimeUnit.SECONDS);
            doReturn(CompletableFuture.failedFuture(BKException.create(BKException.Code.ReadException)))
                    .when(failedHandle).readAsync(position.getEntryId(), position.getEntryId());

            assertThatThrownBy(() -> readEntry(readOnly, position).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);

            assertThat(ledger.ledgerCache.get(position.getLedgerId())).isNull();
            verify(failedHandle).closeAsync();
            readEntry(readOnly, position).get(5, TimeUnit.SECONDS);
            assertThat(ledger.ledgerCache.get(position.getLedgerId()).get(5, TimeUnit.SECONDS))
                    .isNotSameAs(failedHandle);
        } finally {
            cursor.close();
            // The baseline's factory wrapper has no close API; explicitly release its cached handles.
            ledger.invalidateReadHandle(position.getLedgerId());
            ledger.close();
        }
    }

    @Test(dataProvider = "cacheEnabled")
    public void testLateReadFailureDoesNotInvalidateReplacement(boolean cacheEnabled) throws Exception {
        configureCache(cacheEnabled);
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("readonly-late-read-failure", config);
        Position firstPosition = writer.addEntry(new byte[] {1});
        Position secondPosition = writer.addEntry(new byte[] {2});
        writer.close();
        ReadOnlyManagedLedger readOnly = openReadOnly(writer.getName(), config);
        ReadOnlyCursorImpl cursor = (ReadOnlyCursorImpl) readOnly.createReadOnlyCursor(PositionFactory.EARLIEST);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
        CompletableFuture<LedgerEntries> firstStorageRead = new CompletableFuture<>();
        CompletableFuture<LedgerEntries> secondStorageRead = new CompletableFuture<>();
        try {
            readEntry(readOnly, firstPosition).get(5, TimeUnit.SECONDS);
            ReadHandle failedHandle = ledger.ledgerCache.get(firstPosition.getLedgerId()).get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> firstReadStarted = new CompletableFuture<>();
            CompletableFuture<Void> secondReadStarted = new CompletableFuture<>();
            doAnswer(invocation -> {
                firstReadStarted.complete(null);
                return firstStorageRead;
            }).when(failedHandle).readAsync(firstPosition.getEntryId(), firstPosition.getEntryId());
            doAnswer(invocation -> {
                secondReadStarted.complete(null);
                return secondStorageRead;
            }).when(failedHandle).readAsync(secondPosition.getEntryId(), secondPosition.getEntryId());
            CompletableFuture<Void> firstReading = readEntry(readOnly, firstPosition);
            CompletableFuture<Void> secondReading = readEntry(readOnly, secondPosition);
            firstReadStarted.get(5, TimeUnit.SECONDS);
            secondReadStarted.get(5, TimeUnit.SECONDS);

            firstStorageRead.completeExceptionally(BKException.create(BKException.Code.ReadException));
            assertThatThrownBy(() -> firstReading.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);
            assertThat(ledger.ledgerCache.get(firstPosition.getLedgerId())).isNull();
            readEntry(readOnly, firstPosition).get(5, TimeUnit.SECONDS);
            CompletableFuture<ReadHandle> replacement = ledger.ledgerCache.get(firstPosition.getLedgerId());
            ReadHandle replacementHandle = replacement.get(5, TimeUnit.SECONDS);
            assertThat(replacementHandle).isNotSameAs(failedHandle);

            secondStorageRead.completeExceptionally(BKException.create(BKException.Code.ReadException));
            assertThatThrownBy(() -> secondReading.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class);

            assertThat(ledger.ledgerCache.get(firstPosition.getLedgerId())).isSameAs(replacement);
            verify(replacementHandle, never()).closeAsync();
            verify(failedHandle).closeAsync();
            readEntry(readOnly, secondPosition).get(5, TimeUnit.SECONDS);
            assertThat(ledger.ledgerCache.get(firstPosition.getLedgerId())).isSameAs(replacement);
        } finally {
            firstStorageRead.completeExceptionally(BKException.create(BKException.Code.ReadException));
            secondStorageRead.completeExceptionally(BKException.create(BKException.Code.ReadException));
            cursor.close();
            ledger.invalidateReadHandle(firstPosition.getLedgerId());
            ledger.close();
        }
    }

    @Test(dataProvider = "cacheEnabled")
    public void testReadBackpressureKeepsReadOnlyHandle(boolean cacheEnabled) throws Exception {
        configureCache(cacheEnabled);
        ManagedLedgerConfig config = defaultConfig().setRetentionTime(1, TimeUnit.HOURS);
        ManagedLedger writer = factory.open("readonly-read-backpressure", config);
        Position position = writer.addEntry(new byte[] {1});
        writer.close();
        ReadOnlyManagedLedger readOnly = openReadOnly(writer.getName(), config);
        ReadOnlyCursorImpl cursor = (ReadOnlyCursorImpl) readOnly.createReadOnlyCursor(PositionFactory.EARLIEST);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
        try {
            readEntry(readOnly, position).get(5, TimeUnit.SECONDS);
            CompletableFuture<ReadHandle> cached = ledger.ledgerCache.get(position.getLedgerId());
            ReadHandle handle = cached.get(5, TimeUnit.SECONDS);
            doReturn(CompletableFuture.failedFuture(BKException.create(BKException.Code.TooManyRequestsException)))
                    .doCallRealMethod().when(handle).readAsync(position.getEntryId(), position.getEntryId());

            assertThatThrownBy(() -> readEntry(readOnly, position).get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.TooManyRequestsException.class);

            assertThat(ledger.ledgerCache.get(position.getLedgerId())).isSameAs(cached);
            verify(handle, never()).closeAsync();
            readEntry(readOnly, position).get(5, TimeUnit.SECONDS);
            assertThat(ledger.ledgerCache.get(position.getLedgerId())).isSameAs(cached);
        } finally {
            cursor.close();
            ledger.invalidateReadHandle(position.getLedgerId());
            ledger.close();
        }
    }

    private void configureCache(boolean cacheEnabled) throws Exception {
        factory.shutdown();
        ManagedLedgerFactoryConfig config = new ManagedLedgerFactoryConfig();
        if (!cacheEnabled) {
            config.setMaxCacheSize(0);
        }
        factory = new ManagedLedgerFactoryImpl(metadataStore, bkc, config);
    }

    private ReadOnlyManagedLedger openReadOnly(String name, ManagedLedgerConfig config) throws Exception {
        CompletableFuture<ReadOnlyManagedLedger> opening = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(name, new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
            @Override
            public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger ledger, Object ctx) {
                opening.complete(ledger);
            }

            @Override
            public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                opening.completeExceptionally(exception);
            }
        }, config, null);
        return opening.get(5, TimeUnit.SECONDS);
    }

    private CompletableFuture<Void> readEntry(ReadOnlyManagedLedger ledger, Position position) {
        CompletableFuture<Void> reading = new CompletableFuture<>();
        ledger.asyncReadEntry(position, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(Entry entry, Object ctx) {
                entry.release();
                reading.complete(null);
            }

            @Override
            public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                reading.completeExceptionally(exception);
            }
        }, null);
        return reading;
    }


    @Test
    public void testReadOnlyManagedLedgerImplAttachProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        final String propertiesKey = "test-key";
        final String propertiesValue = "test-value";

        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        Map<String, String> properties = new HashMap<>();
        properties.put(propertiesKey, propertiesValue);
        ledger.setProperties(Collections.unmodifiableMap(properties));
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_ATTACHED_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        managedLedger.getProperties().forEach((key, value) -> {
                            assertEquals(key, propertiesKey);
                            assertEquals(value, propertiesValue);
                        });
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, defaultConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

    @Test
    public void testReadOnlyManagedLedgerImplNoProperties()
            throws ManagedLedgerException, InterruptedException, ExecutionException, TimeoutException {
        final ManagedLedger ledger = factory.open(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                defaultConfig().setRetentionTime(1, TimeUnit.HOURS));
        ledger.setConfig(defaultConfig());
        ledger.addEntry("entry-0".getBytes());
        CompletableFuture<Void> future = new CompletableFuture<>();
        factory.asyncOpenReadOnlyManagedLedger(MANAGED_LEDGER_NAME_NON_PROPERTIES,
                new AsyncCallbacks.OpenReadOnlyManagedLedgerCallback() {
                    @Override
                    public void openReadOnlyManagedLedgerComplete(ReadOnlyManagedLedger managedLedger,
                                                                  Object ctx) {
                        assertEquals(managedLedger.getProperties().size(), 0);
                        future.complete(null);
                    }

                    @Override
                    public void openReadOnlyManagedLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        future.completeExceptionally(exception);
                    }
                }, defaultConfig(), null);

        future.get(60, TimeUnit.SECONDS);
    }

}
