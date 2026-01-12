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
package org.apache.bookkeeper.mledger.offload.opendal.provider;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * A configuration wrapper for OpenDAL based tiered storage offloaders.
 *
 * <p>The goal is to keep the same semantics as the current jcloud implementation:
 * resolve both "new" (managedLedgerOffload*) and legacy (s3ManagedLedgerOffload* and gcsManagedLedgerOffload*) keys.
 */
public class OpenDALTieredStorageConfiguration {

    public static final String BLOB_STORE_PROVIDER_KEY = "managedLedgerOffloadDriver";
    public static final String METADATA_FIELD_BUCKET = "bucket";
    public static final String METADATA_FIELD_REGION = "region";
    public static final String METADATA_FIELD_ENDPOINT = "serviceEndpoint";
    public static final String METADATA_FIELD_MAX_BLOCK_SIZE = "maxBlockSizeInBytes";
    public static final String METADATA_FIELD_MIN_BLOCK_SIZE = "minBlockSizeInBytes";
    public static final String METADATA_FIELD_READ_BUFFER_SIZE = "readBufferSizeInBytes";
    public static final String METADATA_FIELD_WRITE_BUFFER_SIZE = "writeBufferSizeInBytes";

    public static final String OFFLOADER_PROPERTY_PREFIX = "managedLedgerOffload";
    public static final String MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC = "maxOffloadSegmentRolloverTimeInSeconds";
    public static final String MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC = "minOffloadSegmentRolloverTimeInSeconds";
    public static final long DEFAULT_MAX_SEGMENT_TIME_IN_SECOND = 600;
    public static final long DEFAULT_MIN_SEGMENT_TIME_IN_SECOND = 0;
    public static final String MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES = "maxOffloadSegmentSizeInBytes";
    public static final long DEFAULT_MAX_SEGMENT_SIZE_IN_BYTES = 1024L * 1024 * 1024;
    public static final String EXTRA_CONFIG_PREFIX = OffloadPoliciesImpl.EXTRA_CONFIG_PREFIX;

    private static final int MB = 1024 * 1024;

    private final Map<String, String> configProperties;

    public static OpenDALTieredStorageConfiguration create(Properties props) {
        Map<String, String> map = props.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), e -> e.getValue().toString()));
        return new OpenDALTieredStorageConfiguration(map);
    }

    public static OpenDALTieredStorageConfiguration create(Map<String, String> props) {
        return new OpenDALTieredStorageConfiguration(props);
    }

    public OpenDALTieredStorageConfiguration(Map<String, String> configProperties) {
        this.configProperties = new HashMap<>(Objects.requireNonNull(configProperties, "configProperties"));
    }

    public Map<String, String> getConfigProperties() {
        return new HashMap<>(configProperties);
    }

    public String getDriver() {
        // Keep the same behavior as tiered-storage-jcloud: default to aws-s3 if not explicitly set.
        return configProperties.getOrDefault(BLOB_STORE_PROVIDER_KEY, "aws-s3");
    }

    public String getScheme() {
        String driver = getDriver();
        if ("aws-s3".equalsIgnoreCase(driver)
                || "s3".equalsIgnoreCase(driver)
                || "aliyun-oss".equalsIgnoreCase(driver)) {
            return "s3";
        }
        if ("google-cloud-storage".equalsIgnoreCase(driver)) {
            return "gcs";
        }
        if ("azureblob".equalsIgnoreCase(driver)) {
            return "azblob";
        }
        if ("transient".equalsIgnoreCase(driver)) {
            return "memory";
        }
        throw new IllegalArgumentException("Unsupported OpenDAL offload driver: " + driver);
    }

    public String getBucket() {
        for (String key : getKeys(METADATA_FIELD_BUCKET)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public String getRegion() {
        for (String key : getKeys(METADATA_FIELD_REGION)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public String getServiceEndpoint() {
        for (String key : getKeys(METADATA_FIELD_ENDPOINT)) {
            if (configProperties.containsKey(key)) {
                return configProperties.get(key);
            }
        }
        return null;
    }

    public long getMaxSegmentTimeInSecond() {
        Long value = getLongFromKeys(
                MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC,
                getKeyName(MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC),
                EXTRA_CONFIG_PREFIX + MAX_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC);
        return value != null ? value : DEFAULT_MAX_SEGMENT_TIME_IN_SECOND;
    }

    public long getMinSegmentTimeInSecond() {
        Long value = getLongFromKeys(
                MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC,
                getKeyName(MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC),
                EXTRA_CONFIG_PREFIX + MIN_OFFLOAD_SEGMENT_ROLLOVER_TIME_SEC);
        return value != null ? value : DEFAULT_MIN_SEGMENT_TIME_IN_SECOND;
    }

    public long getMaxSegmentSizeInBytes() {
        Long value = getLongFromKeys(
                MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES,
                getKeyName(MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES),
                EXTRA_CONFIG_PREFIX + MAX_OFFLOAD_SEGMENT_SIZE_IN_BYTES);
        return value != null ? value : DEFAULT_MAX_SEGMENT_SIZE_IN_BYTES;
    }

    public int getMaxBlockSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_MAX_BLOCK_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.parseInt(configProperties.get(key));
            }
        }
        return 64 * MB;
    }

    public int getMinBlockSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_MIN_BLOCK_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.parseInt(configProperties.get(key));
            }
        }
        return 5 * MB;
    }

    public int getReadBufferSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_READ_BUFFER_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.parseInt(configProperties.get(key));
            }
        }
        return MB;
    }

    public int getWriteBufferSizeInBytes() {
        for (String key : getKeys(METADATA_FIELD_WRITE_BUFFER_SIZE)) {
            if (configProperties.containsKey(key)) {
                return Integer.parseInt(configProperties.get(key));
            }
        }
        return 10 * MB;
    }

    public Map<String, String> getOffloadDriverMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(BLOB_STORE_PROVIDER_KEY, getDriver());
        metadata.put(METADATA_FIELD_BUCKET, StringUtils.defaultString(getBucket()));
        metadata.put(METADATA_FIELD_REGION, StringUtils.defaultString(getRegion()));
        metadata.put(METADATA_FIELD_ENDPOINT, StringUtils.defaultString(getServiceEndpoint()));
        return metadata;
    }

    public Map<String, String> getExtraConfig() {
        // Keep stable ordering for hashing/logging.
        Map<String, String> extra = new TreeMap<>();
        configProperties.forEach((key, value) -> {
            if (!key.startsWith(EXTRA_CONFIG_PREFIX)) {
                return;
            }
            String raw = key.substring(EXTRA_CONFIG_PREFIX.length());
            if (raw.isEmpty()) {
                return;
            }
            extra.put(normalizeExtraConfigKey(raw), value);
        });
        return Collections.unmodifiableMap(extra);
    }

    public void validate() throws IOException {
        String driver = getDriver();
        String bucket = getBucket();
        String region = getRegion();
        String endpoint = getServiceEndpoint();

        if (StringUtils.isBlank(bucket)
                && !"filesystem".equalsIgnoreCase(driver)
                && !"transient".equalsIgnoreCase(driver)) {
            throw new IOException("Bucket cannot be empty for driver " + driver + " offload");
        }

        if ("aws-s3".equalsIgnoreCase(driver)) {
            if (StringUtils.isBlank(region) && StringUtils.isBlank(endpoint)) {
                throw new IOException("Either Region or ServiceEndpoint must be specified for aws-s3 offload");
            }
        } else if ("s3".equalsIgnoreCase(driver) || "aliyun-oss".equalsIgnoreCase(driver)) {
            if (StringUtils.isBlank(endpoint)) {
                throw new IOException("ServiceEndpoint must be specified for driver " + driver + " offload");
            }
        }

        if (getMaxBlockSizeInBytes() < 5 * MB) {
            throw new IOException("managedLedgerOffloadMaxBlockSizeInBytes cannot be less than 5MB");
        }
    }

    List<String> getKeys(String property) {
        String backwardCompatible = getBackwardCompatibleKey(property);
        String modern = getKeyName(property);
        if (StringUtils.isBlank(backwardCompatible)) {
            return List.of(modern);
        }
        return List.of(backwardCompatible, modern);
    }

    private String getKeyName(String property) {
        return OFFLOADER_PROPERTY_PREFIX + StringUtils.capitalize(property);
    }

    private String getBackwardCompatibleKey(String property) {
        String driver = getDriver();
        if ("aws-s3".equalsIgnoreCase(driver)
                || "s3".equalsIgnoreCase(driver)
                || "aliyun-oss".equalsIgnoreCase(driver)) {
            return "s3ManagedLedgerOffload" + StringUtils.capitalize(property);
        }
        if ("google-cloud-storage".equalsIgnoreCase(driver)) {
            return "gcsManagedLedgerOffload" + StringUtils.capitalize(property);
        }
        return null;
    }

    private Long getLongFromKeys(String... keys) {
        for (String key : keys) {
            String value = configProperties.get(key);
            if (value == null) {
                continue;
            }
            return Long.parseLong(value);
        }
        return null;
    }

    static String normalizeExtraConfigKey(String rawKeySuffix) {
        // Some users prefer camelCase keys in broker.conf, while OpenDAL configs are snake_case.
        // We accept both:
        // - if key already contains '_' assume it's snake_case and keep as-is.
        // - otherwise, convert camelCase/PascalCase to snake_case.
        if (rawKeySuffix.indexOf('_') >= 0) {
            return rawKeySuffix;
        }
        StringBuilder out = new StringBuilder(rawKeySuffix.length() + 8);
        for (int i = 0; i < rawKeySuffix.length(); i++) {
            char c = rawKeySuffix.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    out.append('_');
                }
                out.append(Character.toLowerCase(c));
            } else {
                out.append(Character.toLowerCase(c));
            }
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }
}
