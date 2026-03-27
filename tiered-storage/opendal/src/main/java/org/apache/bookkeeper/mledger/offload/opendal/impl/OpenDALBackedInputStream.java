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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.LedgerOffloaderStats;
import org.apache.bookkeeper.mledger.offload.jcloud.BackedInputStream;
import org.apache.bookkeeper.mledger.offload.opendal.storage.OpenDALStorage;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.naming.TopicName;

@Slf4j
class OpenDALBackedInputStream extends BackedInputStream {

    private final OpenDALStorage storage;
    private final String key;
    private final ByteBuf buffer;
    private final long objectLen;
    private final int bufferSize;
    private final LedgerOffloaderStats offloaderStats;
    private final String topicName;

    private long cursor;
    private long bufferOffsetStart;
    private long bufferOffsetEnd;

    OpenDALBackedInputStream(OpenDALStorage storage,
                             String key,
                             long objectLen,
                             int bufferSize,
                             LedgerOffloaderStats offloaderStats,
                             String managedLedgerName) {
        this.storage = storage;
        this.key = key;
        this.buffer = PulsarByteBufAllocator.DEFAULT.buffer(bufferSize, bufferSize);
        this.objectLen = objectLen;
        this.bufferSize = bufferSize;
        this.offloaderStats = offloaderStats;
        this.topicName = managedLedgerName != null ? TopicName.fromPersistenceNamingEncoding(managedLedgerName) : null;
        this.cursor = 0;
        this.bufferOffsetStart = this.bufferOffsetEnd = -1;
    }

    private boolean refillBufferIfNeeded() throws IOException {
        if (buffer.readableBytes() != 0) {
            return true;
        }
        if (cursor >= objectLen) {
            return false;
        }
        long startRange = cursor;
        long endRange = Math.min(cursor + bufferSize - 1, objectLen - 1);
        long startReadTime = System.nanoTime();
        try (InputStream stream = storage.readRange(key, startRange, endRange)) {
            buffer.clear();
            bufferOffsetStart = startRange;
            bufferOffsetEnd = endRange;
            int bytesToCopy = (int) (endRange - startRange + 1);
            fillBuffer(stream, bytesToCopy);
            cursor += buffer.readableBytes();
        } catch (Throwable t) {
            if (offloaderStats != null && topicName != null) {
                offloaderStats.recordReadOffloadError(topicName);
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            }
            throw new IOException("Error reading from OpenDAL", t);
        } finally {
            if (offloaderStats != null && topicName != null) {
                offloaderStats.recordReadOffloadDataLatency(topicName,
                        System.nanoTime() - startReadTime, TimeUnit.NANOSECONDS);
                offloaderStats.recordReadOffloadBytes(topicName, endRange - startRange + 1);
            }
        }
        return true;
    }

    private void fillBuffer(InputStream is, int bytesToCopy) throws IOException {
        while (bytesToCopy > 0) {
            int writeBytes = buffer.writeBytes(is, bytesToCopy);
            if (writeBytes < 0) {
                break;
            }
            bytesToCopy -= writeBytes;
        }
    }

    @Override
    public int read() throws IOException {
        if (refillBufferIfNeeded()) {
            return buffer.readUnsignedByte();
        }
        return -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (refillBufferIfNeeded()) {
            int bytesToRead = Math.min(len, buffer.readableBytes());
            buffer.readBytes(b, off, bytesToRead);
            return bytesToRead;
        }
        return -1;
    }

    @Override
    public void seek(long position) {
        if (log.isDebugEnabled()) {
            log.debug("Seeking to {} on {}, current position {} (bufStart:{}, bufEnd:{})",
                    position, key, cursor, bufferOffsetStart, bufferOffsetEnd);
        }
        if (position >= bufferOffsetStart && position <= bufferOffsetEnd) {
            long newIndex = position - bufferOffsetStart;
            buffer.readerIndex((int) newIndex);
        } else {
            bufferOffsetStart = bufferOffsetEnd = -1;
            cursor = position;
            buffer.clear();
        }
    }

    @Override
    public void seekForward(long position) throws IOException {
        if (position >= cursor) {
            seek(position);
        } else {
            throw new IOException(String.format("Error seeking, new position %d < current position %d",
                    position, cursor));
        }
    }

    @Override
    public long getCurrentPosition() {
        if (bufferOffsetStart != -1) {
            return bufferOffsetStart + buffer.readerIndex();
        }
        return cursor + buffer.readerIndex();
    }

    @Override
    public void close() {
        buffer.release();
    }

    @Override
    public int available() {
        long available = objectLen - cursor + buffer.readableBytes();
        return available > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) available;
    }
}

