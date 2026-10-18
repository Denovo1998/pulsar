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
package org.apache.bookkeeper.meta;

import static org.apache.bookkeeper.mledger.util.ManagedLedgerTestUtil.defaultConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.util.Futures.CloseFuture;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.awaitility.Awaitility;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

// This package allows checking BookKeeper's actual listener registry without reflection or a mock registry.
public class FencedManagedLedgerCursorTest extends BookKeeperClusterTestCase {
    public FencedManagedLedgerCursorTest() {
        super(2);
    }

    @DataProvider
    public Object[][] recoveryOrdering() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "recoveryOrdering", timeOut = 60000)
    public void testFencedCloseReleasesRecoveredCursorListener(boolean recoveryBeforeFencing) throws Exception {
        String name = "fenced-cursor-" + UUID.randomUUID();
        ManagedLedgerConfig config = defaultConfig().setEnsembleSize(2).setWriteQuorumSize(2).setAckQuorumSize(2)
                .setMaxUnackedRangesToPersistInMetadataStore(0).setThrottleMarkDelete(0)
                .setRetentionTime(1, TimeUnit.DAYS).setRetentionSizeInMB(-1);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl initialFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ManagedLedgerImpl initialLedger = (ManagedLedgerImpl) initialFactory.open(name, config);
        ManagedCursorImpl initialCursor = (ManagedCursorImpl) initialLedger.openCursor("cursor");
        Position first = initialLedger.addEntry(new byte[] {1});
        Position second = initialLedger.addEntry(new byte[] {2});
        initialLedger.addEntry(new byte[] {3});
        initialCursor.delete(second);
        long cursorLedgerId = initialCursor.getCursorLedger();
        assertThat(cursorLedgerId).isNotEqualTo(-1);
        // An individual ACK hole keeps the cursor snapshot in BookKeeper on normal close.
        initialLedger.close();

        CompletableFuture<Void> resumeRecovery = new CompletableFuture<>();
        CompletableFuture<ReadHandle> recoveryHandle = new CompletableFuture<>();
        BookKeeper observedBookKeeper = delayCursorOpen(cursorLedgerId, resumeRecovery, recoveryHandle);
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl oldFactory = new ManagedLedgerFactoryImpl(metadataStore, observedBookKeeper);
        ManagedLedgerImpl oldLedger = (ManagedLedgerImpl) oldFactory.open(name, config.setLazyCursorRecovery(true));
        ReadHandle handle = recoveryHandle.get(10, TimeUnit.SECONDS);
        CompletableFuture<ManagedCursor> recovery = new CompletableFuture<>();
        oldLedger.asyncOpenCursor("cursor", new AsyncCallbacks.OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                recovery.complete(cursor);
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                recovery.completeExceptionally(exception);
            }
        }, null);
        try {
            assertThat(hasListener(cursorLedgerId, handle)).isTrue();
            assertThat(recovery).isNotDone();
            ManagedCursor recovered = null;
            if (recoveryBeforeFencing) {
                resumeRecovery.complete(null);
                recovered = recovery.get(10, TimeUnit.SECONDS);
            }

            // A second real owner recovers the old owner's current ledger. Its next write fails through
            // BookKeeper's fencing/recheck path, placing the managed ledger itself in Fenced state.
            @Cleanup("shutdown")
            ManagedLedgerFactoryImpl successorFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
            ManagedLedgerImpl successor = (ManagedLedgerImpl) successorFactory.open(name,
                    config.setLazyCursorRecovery(false));
            ManagedCursor successorCursor = successor.openCursor("cursor");
            assertThatThrownBy(() -> oldLedger.addEntry(new byte[] {4}))
                    .isInstanceOf(ManagedLedgerException.class);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> oldLedger.getState().isFenced());
            String cursorPath = "/managed-ledgers/" + name + "/cursor";
            var metadataBeforeClose = metadataStore.get(cursorPath).get(10, TimeUnit.SECONDS).orElseThrow();

            CloseFuture closing = new CloseFuture();
            oldLedger.asyncClose(closing, null);
            assertThatThrownBy(() -> closing.get(10, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerFencedException.class);
            if (!recoveryBeforeFencing) {
                // The BookKeeper open really completed and registered its listener before close. Only delivery
                // of that result to cursor recovery is delayed; no ManagedLedger callback is invoked by the test.
                resumeRecovery.complete(null);
            }
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(hasListener(cursorLedgerId, handle))
                            .as("fenced owner must unregister the recovered cursor's listener").isFalse());
            if (recoveryBeforeFencing) {
                assertThat(recovered.isClosed()).isTrue();
            } else {
                assertThatThrownBy(() -> recovery.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerFencedException.class);
                assertThat(oldLedger.getCursors()).isEmpty();
            }
            // Repeated close must still report fencing without persisting stale cursor metadata.
            assertThatThrownBy(oldLedger::close).isInstanceOf(ManagedLedgerFencedException.class);
            var metadataAfterClose = metadataStore.get(cursorPath).get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(metadataAfterClose.getValue()).isEqualTo(metadataBeforeClose.getValue());
            assertThat(metadataAfterClose.getStat().getVersion()).isEqualTo(metadataBeforeClose.getStat().getVersion());
            bkc.getLedgerManager().readLedgerMetadata(cursorLedgerId).get(10, TimeUnit.SECONDS);
            assertThat(successorCursor.isMessageDeleted(first)).isFalse();
            assertThat(successorCursor.isMessageDeleted(second)).isTrue();
            List<Entry> entries = successorCursor.readEntries(10);
            try {
                assertThat(entries).isNotEmpty();
                assertThat(entries.get(0).getPosition()).isEqualTo(first);
            } finally {
                entries.forEach(Entry::release);
            }
        } finally {
            resumeRecovery.complete(null);
            // Keep the shared BookKeeper client alive until all lifecycle assertions above have run.
            handle.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("unchecked")
    private BookKeeper delayCursorOpen(long cursorLedgerId, CompletableFuture<Void> resume,
                                      CompletableFuture<ReadHandle> recoveryHandle) {
        BookKeeper observed = spy(bkc);
        doAnswer(invocation -> {
            OpenBuilder builder = spy((OpenBuilder) invocation.callRealMethod());
            doAnswer(open -> ((CompletableFuture<ReadHandle>) open.callRealMethod()).thenCompose(handle -> {
                if (handle.getId() == cursorLedgerId) {
                    recoveryHandle.complete(handle);
                    return resume.thenApply(ignored -> handle);
                }
                return CompletableFuture.completedFuture(handle);
            })).when(builder).execute();
            return builder;
        }).when(observed).newOpenLedgerOp();
        return observed;
    }

    private boolean hasListener(long ledgerId, ReadHandle handle) {
        LedgerManager manager = bkc.getLedgerManager();
        while (manager instanceof CleanupLedgerManager cleanup) {
            manager = cleanup.getUnderlying();
        }
        Set<LedgerMetadataListener> listeners = ((AbstractZkLedgerManager) manager).listeners.get(ledgerId);
        if (listeners == null) {
            return false;
        }
        synchronized (listeners) {
            return listeners.contains(handle);
        }
    }
}
