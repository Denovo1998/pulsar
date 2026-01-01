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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALOperatorProvider;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OpenDALTieredStorageConfiguration;
import org.apache.bookkeeper.mledger.offload.opendal.provider.OperatorCache;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class OpenDALStorageTest {

    private OperatorCache operatorCache;

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        if (operatorCache != null) {
            operatorCache.close();
        }
    }

    @Test
    public void testWriteReadStatListDeleteOnMemoryBackend() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("managedLedgerOffloadDriver", "transient");

        OpenDALTieredStorageConfiguration config = OpenDALTieredStorageConfiguration.create(props);
        operatorCache = new OperatorCache();
        OpenDALOperatorProvider provider = new OpenDALOperatorProvider(config, operatorCache);

        OpenDALStorage storage = new OpenDALStorage(provider, Collections.emptyMap());

        String key = "k1";
        byte[] payload = "hello-opendal".getBytes(UTF_8);
        storage.writeBytes(key, payload, Map.of("role", "test"));

        OpenDALStorage.ObjectMetadata metadata = storage.stat(key);
        assertEquals(metadata.getSize(), payload.length);

        try (InputStream in = storage.readRange(key, 0, payload.length - 1)) {
            assertEquals(new String(in.readAllBytes(), UTF_8), "hello-opendal");
        }

        OpenDALStorage.ListResult list = storage.list("", null, 100);
        assertTrue(list.getItems().stream().anyMatch(i -> key.equals(i.getPath())));

        storage.delete(key);
        try {
            storage.stat(key);
        } catch (FileNotFoundException expected) {
            // ok
        }
    }
}
