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
package org.apache.bookkeeper.mledger.offload.opendal.storage;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALOperatorProvider;
import org.apache.opendal.Entry;
import org.apache.opendal.ListOptions;
import org.apache.opendal.Metadata;
import org.apache.opendal.OpenDALException;
import org.apache.opendal.Operator;
import org.apache.opendal.WriteOptions;

/**
 * A thin wrapper around the OpenDAL Java binding.
 *
 * <p>All OpenDAL calls should be kept inside this class so that the offloader logic layer can stay clean
 * (and provider backends can be changed without touching core offload algorithms).
 */
@Slf4j
public class OpenDALStorage {

    @Value
    public static class ObjectMetadata {
        long size;
        Instant lastModified;
    }

    @Value
    public static class ListResult {
        List<Item> items;
        String nextMarker;
    }

    @Value
    public static class Item {
        String path;
        ObjectMetadata metadata;
    }

    private final OpenDALOperatorProvider operatorProvider;
    private final Map<String, String> offloadDriverMetadata;

    public OpenDALStorage(OpenDALOperatorProvider operatorProvider, Map<String, String> offloadDriverMetadata) {
        this.operatorProvider = Objects.requireNonNull(operatorProvider, "operatorProvider");
        this.offloadDriverMetadata = Objects.requireNonNull(offloadDriverMetadata, "offloadDriverMetadata");
    }

    public OutputStream openOutputStream(String key) throws IOException {
        Operator operator = operatorProvider.getOperator(offloadDriverMetadata);
        try {
            OutputStream out = operator.createOutputStream(key);
            return new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                }

                @Override
                public void flush() throws IOException {
                    out.flush();
                }

                @Override
                public void close() throws IOException {
                    try {
                        out.close();
                    } finally {
                        operator.close();
                    }
                }
            };
        } catch (Throwable t) {
            operator.close();
            throw toIOException("openOutputStream", key, t);
        }
    }

    public void writeBytes(String key, byte[] data, Map<String, String> userMetadata) throws IOException {
        Map<String, String> safeUserMeta = (userMetadata != null) ? userMetadata : Collections.emptyMap();
        WriteOptions options = WriteOptions.builder()
                .contentType("application/octet-stream")
                .userMetadata(safeUserMeta)
                .build();

        try (Operator operator = operatorProvider.getOperator(offloadDriverMetadata)) {
            operator.write(key, data, options);
        } catch (Throwable t) {
            throw toIOException("writeBytes", key, t);
        }
    }

    public InputStream readRange(String key, long startInclusive, long endInclusive) throws IOException {
        if (startInclusive < 0 || endInclusive < startInclusive) {
            throw new IllegalArgumentException("Invalid range: " + startInclusive + "-" + endInclusive);
        }
        long length = endInclusive - startInclusive + 1;
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Range too large: " + length);
        }
        try (Operator operator = operatorProvider.getOperator(offloadDriverMetadata)) {
            byte[] data = operator.read(key, startInclusive, length);
            return new ByteArrayInputStream(data);
        } catch (Throwable t) {
            throw toIOException("readRange", key, t);
        }
    }

    public ObjectMetadata stat(String key) throws IOException {
        try (Operator operator = operatorProvider.getOperator(offloadDriverMetadata)) {
            Metadata md = operator.stat(key);
            return new ObjectMetadata(md.getContentLength(), md.getLastModified());
        } catch (Throwable t) {
            throw toIOException("stat", key, t);
        }
    }

    public void delete(String key) throws IOException {
        try (Operator operator = operatorProvider.getOperator(offloadDriverMetadata)) {
            operator.delete(key);
        } catch (Throwable t) {
            throw toIOException("delete", key, t);
        }
    }

    public void delete(List<String> keys) throws IOException {
        for (String key : keys) {
            delete(key);
        }
    }

    public ListResult list(String prefix, String marker, long limit) throws IOException {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be > 0");
        }
        try (Operator operator = operatorProvider.getOperator(offloadDriverMetadata)) {
            ListOptions.ListOptionsBuilder options = ListOptions.builder().recursive(true).limit(limit);
            if (marker != null && !marker.isEmpty()) {
                options.startAfter(marker);
            }
            List<Entry> entries = operator.list(prefix == null ? "" : prefix, options.build());
            List<Item> items = entries.stream()
                    .filter(e -> e.getMetadata() != null && e.getMetadata().isFile())
                    .map(e -> new Item(e.getPath(), new ObjectMetadata(
                            e.getMetadata().getContentLength(), e.getMetadata().getLastModified())))
                    .collect(Collectors.toList());

            String nextMarker = null;
            if (items.size() == limit) {
                nextMarker = Optional.ofNullable(items.get(items.size() - 1).getPath()).orElse(null);
            }
            return new ListResult(items, nextMarker);
        } catch (Throwable t) {
            throw toIOException("list", prefix, t);
        }
    }

    private static IOException toIOException(String op, String key, Throwable t) {
        if (t instanceof OpenDALException) {
            OpenDALException e = (OpenDALException) t;
            if (e.getCode() == OpenDALException.Code.NotFound) {
                return new FileNotFoundException(op + " not found: " + key);
            }
        }
        if (t instanceof IOException) {
            return (IOException) t;
        }
        return new IOException("OpenDAL " + op + " failed for key " + key, t);
    }
}
