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
package org.apache.bookkeeper.mledger.offload.opendal;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.LedgerOffloaderFactory;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.LedgerOffloaderStatsDisable;
import org.apache.bookkeeper.mledger.offload.jcloud.impl.OffsetsCache;
import org.apache.bookkeeper.mledger.offload.opendal.impl.OpenDALManagedLedgerOffloader;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALOperatorProvider;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALTieredStorageConfiguration;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OperatorCache;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * OpenDAL based offloader factory.
 *
 * <p>This module is introduced as a skeleton first, to make sure the NAR can be discovered and loaded.
 * The actual offloader implementation will be provided in follow-up PRs.
 */
public class OpenDALLedgerOffloaderFactory implements LedgerOffloaderFactory<OpenDALManagedLedgerOffloader> {

    // Keep aligned with the existing driver names so that broker configs don't need to change.
    private static final Set<String> SUPPORTED_DRIVERS = Set.of(
            "aws-s3",
            "S3",
            "aliyun-oss",
            "google-cloud-storage",
            "azureblob",
            "transient"
    );

    private final OffsetsCache entryOffsetsCache = new OffsetsCache();
    private final OperatorCache operatorCache = new OperatorCache();

    @Override
    public boolean isDriverSupported(String driverName) {
        return driverName != null && SUPPORTED_DRIVERS.stream().anyMatch(d -> d.equalsIgnoreCase(driverName));
    }

    @Override
    public OpenDALManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies,
                                                Map<String, String> userMetadata,
                                                OrderedScheduler scheduler) throws IOException {
        return create(offloadPolicies, userMetadata, scheduler, LedgerOffloaderStatsDisable.INSTANCE);
    }

    @Override
    public OpenDALManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies,
                                                Map<String, String> userMetadata,
                                                OrderedScheduler scheduler,
                                                LedgerOffloaderStats offloaderStats) throws IOException {
        return create(offloadPolicies, userMetadata, scheduler, scheduler, offloaderStats);
    }

    @Override
    public OpenDALManagedLedgerOffloader create(OffloadPoliciesImpl offloadPolicies,
                                                Map<String, String> userMetadata,
                                                OrderedScheduler scheduler,
                                                OrderedScheduler readExecutor,
                                                LedgerOffloaderStats offloaderStats) throws IOException {
        OpenDALTieredStorageConfiguration config =
                OpenDALTieredStorageConfiguration.create(offloadPolicies.toProperties());
        OpenDALOperatorProvider operatorProvider = new OpenDALOperatorProvider(config, operatorCache);
        return OpenDALManagedLedgerOffloader.create(config, userMetadata, scheduler, readExecutor, offloaderStats,
                entryOffsetsCache, operatorProvider);
    }

    @Override
    public void close() throws Exception {
        operatorCache.close();
        entryOffsetsCache.close();
    }
}
