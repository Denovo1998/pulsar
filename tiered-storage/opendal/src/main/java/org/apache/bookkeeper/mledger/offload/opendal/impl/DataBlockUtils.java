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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Format and naming utilities that must remain compatible with the existing tiered-storage-jcloud implementation.
 */
public final class DataBlockUtils {

    /**
     * Keep the same metadata key used by tiered-storage-jcloud.
     *
     * <p>Note: Some backends normalize user-metadata keys; callers should treat it as case-insensitive.
     */
    public static final String METADATA_FORMAT_VERSION_KEY = "S3ManagedLedgerOffloaderFormatVersion";

    /**
     * Keep the same format version written by tiered-storage-jcloud.
     */
    static final String CURRENT_VERSION = String.valueOf(1);

    private DataBlockUtils() {
    }

    public static String dataBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d", uuid.toString(), ledgerId);
    }

    public static String indexBlockOffloadKey(long ledgerId, UUID uuid) {
        return String.format("%s-ledger-%d-index", uuid.toString(), ledgerId);
    }

    public static String indexBlockOffloadKey(UUID uuid) {
        return String.format("%s-index", uuid.toString());
    }

    public static Map<String, String> withVersionInfo(Map<String, String> userMetadata) {
        Map<String, String> metadata = new HashMap<>();
        if (userMetadata != null && !userMetadata.isEmpty()) {
            metadata.putAll(userMetadata);
        }
        // Follow tiered-storage-jcloud behavior: write the version key using lower-case.
        metadata.put(METADATA_FORMAT_VERSION_KEY.toLowerCase(), CURRENT_VERSION);
        return metadata;
    }

    public static Long parseLedgerId(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        if (name.endsWith("-index")) {
            name = name.substring(0, name.length() - "-index".length());
        }
        int pos = name.indexOf("-ledger-");
        if (pos < 0) {
            return null;
        }
        try {
            return Long.parseLong(name.substring(pos + 8));
        } catch (NumberFormatException err) {
            return null;
        }
    }

    public static String parseContextUuid(String name, Long ledgerId) {
        if (ledgerId == null || name == null) {
            return null;
        }
        int pos = name.indexOf("-ledger-" + ledgerId);
        if (pos <= 0) {
            return null;
        }
        return name.substring(0, pos);
    }
}

