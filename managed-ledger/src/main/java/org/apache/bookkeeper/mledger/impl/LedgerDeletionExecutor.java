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

import com.google.common.util.concurrent.ForwardingExecutorService;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;

/**
 * Notifies managed-ledger deletions discarded from the executor queue during shutdown.
 * The delegate must return the original queued tasks from {@link #shutdownNow()}.
 */
@CustomLog
public final class LedgerDeletionExecutor extends ForwardingExecutorService {
    private final ExecutorService delegate;

    public LedgerDeletionExecutor(ExecutorService delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    protected ExecutorService delegate() {
        return delegate;
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> pending = delegate.shutdownNow();
        for (Runnable task : pending) {
            if (task instanceof DeletionTask deletionTask) {
                try {
                    deletionTask.reject();
                } catch (Throwable t) {
                    // One failed notification must not leave the remaining deletions pending.
                    log.debug().exception(t).log("Ledger deletion shutdown callback failed");
                }
            }
        }
        return pending;
    }

    static final class DeletionTask implements Runnable {
        private final Runnable action;
        private final Runnable rejected;
        private final AtomicBoolean claimed = new AtomicBoolean();

        DeletionTask(Runnable action, Runnable rejected) {
            this.action = action;
            this.rejected = rejected;
        }

        @Override
        public void run() {
            if (claimed.compareAndSet(false, true)) {
                action.run();
            }
        }

        void reject() {
            if (claimed.compareAndSet(false, true)) {
                rejected.run();
            }
        }
    }
}
