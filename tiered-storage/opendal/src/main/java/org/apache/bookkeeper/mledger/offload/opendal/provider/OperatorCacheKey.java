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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

final class OperatorCacheKey {
    private static final HexFormat HEX = HexFormat.of();

    private final String scheme;
    private final String bucket;
    private final String region;
    private final String endpoint;
    private final String configHash;

    static OperatorCacheKey of(String scheme,
                               String bucket,
                               String region,
                               String endpoint,
                               Map<String, String> operatorConfig) {
        return new OperatorCacheKey(scheme, bucket, region, endpoint, sha256Hex(operatorConfig));
    }

    private OperatorCacheKey(String scheme, String bucket, String region, String endpoint, String configHash) {
        this.scheme = scheme;
        this.bucket = bucket;
        this.region = region;
        this.endpoint = endpoint;
        this.configHash = configHash;
    }

    private static String sha256Hex(Map<String, String> operatorConfig) {
        Map<String, String> sorted = new TreeMap<>(operatorConfig);
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory in every Java runtime.
            throw new RuntimeException(e);
        }
        sorted.forEach((key, value) -> {
            digest.update(key.getBytes(StandardCharsets.UTF_8));
            digest.update((byte) '=');
            if (value != null) {
                digest.update(value.getBytes(StandardCharsets.UTF_8));
            }
            digest.update((byte) '\n');
        });
        return HEX.formatHex(digest.digest());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OperatorCacheKey)) {
            return false;
        }
        OperatorCacheKey that = (OperatorCacheKey) o;
        return Objects.equals(scheme, that.scheme)
                && Objects.equals(bucket, that.bucket)
                && Objects.equals(region, that.region)
                && Objects.equals(endpoint, that.endpoint)
                && Objects.equals(configHash, that.configHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scheme, bucket, region, endpoint, configHash);
    }

    @Override
    public String toString() {
        return "OperatorCacheKey{"
                + "scheme='" + scheme + '\''
                + ", bucket='" + bucket + '\''
                + ", region='" + region + '\''
                + ", endpoint='" + endpoint + '\''
                + ", configHash='" + configHash + '\''
                + '}';
    }
}

