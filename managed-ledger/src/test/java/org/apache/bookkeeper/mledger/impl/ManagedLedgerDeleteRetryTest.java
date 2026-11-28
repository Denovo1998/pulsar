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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.util.PulsarExecutors;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerDeleteRetryTest extends MockedBookKeeperTestCase {

    @DataProvider(name = "queuedDeleteCallbackFailure")
    public Object[][] queuedDeleteCallbackFailure() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "queuedDeleteCallbackFailure", timeOut = 20_000)
    public void testManagedLedgerDeleteFailsForTasksDrainedDuringShutdown(boolean throwFromQueuedCallback)
            throws Exception {
        Semaphore semaphore = new Semaphore(1);
        ExecutorService deleteExecutor = new LedgerDeletionExecutor(
                PulsarExecutors.newSingleThreadExecutor(new DefaultThreadFactory("queued-ledger-delete")));
        ManagedLedgerConfig config = defaultConfig().setLedgerDeletionSemaphore(semaphore)
                .setLedgerDeleteExecutor(deleteExecutor);
        List<ManagedLedgerImpl> ledgers = new ArrayList<>();
        List<CompletableFuture<Void>> results = new ArrayList<>();
        AtomicIntegerArray callbacks = new AtomicIntegerArray(3);
        semaphore.acquire();
        try {
            for (int i = 0; i < 3; i++) {
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("delete-queued-" + i, config);
                ledger.addEntry(new byte[] {1});
                ledgers.add(ledger);
                results.add(new CompletableFuture<>());
            }
            for (int i = 0; i < ledgers.size(); i++) {
                int index = i;
                ledgers.get(i).asyncDelete(new DeleteLedgerCallback() {
                    @Override
                    public void deleteLedgerComplete(Object ctx) {
                        callbacks.incrementAndGet(index);
                        results.get(index).complete(null);
                    }

                    @Override
                    public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                        callbacks.incrementAndGet(index);
                        results.get(index).completeExceptionally(exception);
                        if (throwFromQueuedCallback && index == 1) {
                            throw new IllegalStateException("delete callback failed");
                        }
                    }
                }, null);
                if (i == 0) {
                    // The first deletion occupies the sole worker; subsequent deletions must remain queued.
                    await().atMost(5, TimeUnit.SECONDS).until(semaphore::hasQueuedThreads);
                }
            }

            assertThat(deleteExecutor.shutdownNow()).hasSize(2);
            assertThat(deleteExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            for (int i = 0; i < results.size(); i++) {
                CompletableFuture<Void> result = results.get(i);
                int errorCode = i == 0 ? BKException.Code.InterruptedException : BKException.Code.ClientClosedException;
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(ManagedLedgerException.class)
                        .hasRootCauseMessage(BKException.getMessage(errorCode) + " error code: " + errorCode);
                assertThat(callbacks.get(i)).isEqualTo(1);
                ManagedLedgerImpl ledger = ledgers.get(i);
                assertThat(bkc.getLedgers()).contains(ledger.getCurrentLedger().getId());
                assertThat(metadataStore.exists("/managed-ledgers/" + ledger.getName()).get(5, TimeUnit.SECONDS))
                        .isTrue();
            }
            assertThat(semaphore.availablePermits()).isZero();
        } finally {
            semaphore.release();
            deleteExecutor.shutdownNow();
            assertThat(deleteExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test(timeOut = 15_000)
    public void testManagedLedgerDeleteReleasesAcquiredPermit() throws Exception {
        Semaphore semaphore = new Semaphore(1);
        ExecutorService deleteExecutor = new LedgerDeletionExecutor(
                PulsarExecutors.newSingleThreadExecutor(new DefaultThreadFactory("successful-ledger-delete")));
        try {
            ManagedLedgerConfig config = defaultConfig().setLedgerDeletionSemaphore(semaphore)
                    .setLedgerDeleteExecutor(deleteExecutor);
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("delete-with-permit", config);
            ledger.addEntry(new byte[] {1});
            long ledgerId = ledger.getCurrentLedger().getId();

            ledger.delete();

            assertThat(semaphore.availablePermits()).isEqualTo(1);
            assertThat(bkc.getLedgers()).doesNotContain(ledgerId);
            assertThat(metadataStore.exists("/managed-ledgers/" + ledger.getName()).get(5, TimeUnit.SECONDS)).isFalse();
        } finally {
            deleteExecutor.shutdownNow();
            assertThat(deleteExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @DataProvider(name = "deleteExecutorShutdown")
    public Object[][] deleteExecutorShutdown() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "deleteExecutorShutdown", timeOut = 15_000)
    public void testManagedLedgerDeleteFailsWhenDeleteExecutorShutsDown(boolean interruptWaitingTask) throws Exception {
        Semaphore semaphore = new Semaphore(1);
        ExecutorService deleteExecutor = new LedgerDeletionExecutor(
                PulsarExecutors.newSingleThreadExecutor(new DefaultThreadFactory("ledger-delete")));
        ManagedLedgerConfig config = defaultConfig().setLedgerDeletionSemaphore(semaphore)
                .setLedgerDeleteExecutor(deleteExecutor);
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open("delete-executor-shutdown", config);
        ledger.addEntry(new byte[] {1});
        long ledgerId = ledger.getCurrentLedger().getId();
        CompletableFuture<Void> result = new CompletableFuture<>();
        AtomicBoolean callbackInterrupted = new AtomicBoolean();
        AtomicInteger callbacks = new AtomicInteger();
        // Hold the only permit so the accepted deletion task waits in acquire() until shutdownNow interrupts it.
        semaphore.acquire();
        try {
            if (!interruptWaitingTask) {
                deleteExecutor.shutdownNow();
            }
            ledger.asyncDelete(new DeleteLedgerCallback() {
                @Override
                public void deleteLedgerComplete(Object ctx) {
                    callbacks.incrementAndGet();
                    result.complete(null);
                }

                @Override
                public void deleteLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    callbacks.incrementAndGet();
                    callbackInterrupted.set(Thread.currentThread().isInterrupted());
                    result.completeExceptionally(exception);
                }
            }, null);
            if (interruptWaitingTask) {
                await().atMost(5, TimeUnit.SECONDS).until(semaphore::hasQueuedThreads);
                deleteExecutor.shutdownNow();
            }

            int errorCode = interruptWaitingTask ? BKException.Code.InterruptedException
                    : BKException.Code.ClientClosedException;
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ManagedLedgerException.class)
                    .hasRootCauseMessage(BKException.getMessage(errorCode) + " error code: " + errorCode);
            assertThat(deleteExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(callbacks.get()).isEqualTo(1);
            assertThat(callbackInterrupted.get()).isEqualTo(interruptWaitingTask);
            assertThat(semaphore.availablePermits()).isZero();
            assertThat(bkc.getLedgers()).contains(ledgerId);
            assertThat(metadataStore.exists("/managed-ledgers/" + ledger.getName()).get(5, TimeUnit.SECONDS)).isTrue();
        } finally {
            semaphore.release();
            deleteExecutor.shutdownNow();
            assertThat(deleteExecutor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(semaphore.availablePermits()).isEqualTo(1);
    }

    @Test(timeOut = 15_000)
    public void testOriginalFutureCompletesWhenRetrySchedulingIsRejected() throws Exception {
        LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        long ledgerId = handle.getId();
        handle.close();
        OrderedScheduler retryScheduler = OrderedScheduler.newSchedulerBuilder()
                .numThreads(1).name("delete-retry-rejected").build();
        try {
            ManagedLedgerImpl ledger = new ManagedLedgerImpl(factory, bkc, factory.getMetaStore(), defaultConfig(),
                    retryScheduler, "delete-retry-rejected");
            retryScheduler.shutdownNow();
            bkc.failNow(BKException.Code.WriteException);

            CompletableFuture<Void> result = ledger.asyncDeleteLedger(ledgerId,
                    ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES);

            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(RejectedExecutionException.class);
            assertThat(bkc.getLedgers()).contains(ledgerId);
        } finally {
            retryScheduler.shutdownNow();
        }
    }

    @DataProvider(name = "deleteFailures")
    public Object[][] deleteFailures() {
        return new Object[][] {{0, false}, {1, false}, {2, false}, {3, false}, {1, true}};
    }

    @Test(dataProvider = "deleteFailures", timeOut = 15_000)
    public void testOriginalFutureCompletesAfterDeleteRetries(int failures, boolean deleteBeforeRetry)
            throws Exception {
        LedgerHandle handle = bkc.createLedger(BookKeeper.DigestType.CRC32C, new byte[0]);
        long ledgerId = handle.getId();
        handle.close();
        OrderedScheduler retryScheduler = mock(OrderedScheduler.class);
        AtomicInteger scheduledRetries = new AtomicInteger();
        // Execute the real retry tasks without waiting for the production backoff. Support both overloads
        // so the regression also exercises the original implementation, which schedules a Callable.
        doAnswer(invocation -> {
            scheduledRetries.incrementAndGet();
            if (deleteBeforeRetry) {
                // The first attempt failed, but the ledger is gone when the retry runs:
                // PulsarMockBookKeeper will return NoSuchLedgerExistsException.
                bkc.deleteLedger(ledgerId);
            }
            Runnable task = invocation.getArgument(0);
            return executor.schedule(task, 0, TimeUnit.SECONDS);
        }).when(retryScheduler).schedule(any(Runnable.class),
                eq((long) ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC), eq(TimeUnit.SECONDS));
        doAnswer(invocation -> {
            scheduledRetries.incrementAndGet();
            if (deleteBeforeRetry) {
                bkc.deleteLedger(ledgerId);
            }
            Callable<?> task = invocation.getArgument(0);
            return executor.schedule(task, 0, TimeUnit.SECONDS);
        }).when(retryScheduler).schedule(ArgumentMatchers.<Callable<?>>any(),
                eq((long) ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_BACKOFF_TIME_SEC), eq(TimeUnit.SECONDS));

        ManagedLedgerImpl ledger = new ManagedLedgerImpl(factory, bkc, factory.getMetaStore(), defaultConfig(),
                retryScheduler, "delete-retry");
        for (int i = 0; i < failures; i++) {
            bkc.failAfter(i, BKException.Code.WriteException);
        }

        CompletableFuture<Void> result = ledger.asyncDeleteLedger(ledgerId,
                ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES);
        if (failures == ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES) {
            assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(BKException.BKWriteException.class);
            assertThat(bkc.getLedgers()).contains(ledgerId);
        } else {
            await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(bkc.getLedgers()).doesNotContain(ledgerId));
            result.get(5, TimeUnit.SECONDS);
        }
        assertThat(scheduledRetries.get()).isEqualTo(
                Math.min(failures, ManagedLedgerImpl.DEFAULT_LEDGER_DELETE_RETRIES - 1));
    }
}
