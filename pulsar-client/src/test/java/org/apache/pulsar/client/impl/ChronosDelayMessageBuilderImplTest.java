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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayMeta;
import com.xiaojukeji.carrera.chronos.protocol.ChronosDelayType;
import com.xiaojukeji.carrera.chronos.protocol.ChronosSendResult;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class ChronosDelayMessageBuilderImplTest {

    @Test
    public void testBuilderSendsOnceAndMergesProperties() {
        ChronosMessageSender<String> sender = mock(ChronosMessageSender.class);
        ChronosSendResult success = new ChronosSendResult(ChronosSendResult.OK, "OK", "uniq");
        when(sender.sendAsync(any(), any(), any(), anyMap())).thenReturn(CompletableFuture.completedFuture(success));

        ChronosDelayMessageBuilderImpl<String> builder = new ChronosDelayMessageBuilderImpl<>(sender);
        ChronosDelayMeta delayMeta = new ChronosDelayMeta()
                .setTimestamp(System.currentTimeMillis() / 1000 + 60)
                .setDelayType(ChronosDelayType.DELAY)
                .setTimes(0)
                .setInterval(0);

        ChronosSendResult first = builder.value("hello")
                .delayMeta(delayMeta)
                .property("traceId", "a")
                .properties(Map.of("traceId", "b", "region", "hz"))
                .tags("old")
                .tags("tagA", "tagB")
                .send();

        ArgumentCaptor<Map<String, String>> propertiesCaptor = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<String[]> tagsCaptor = ArgumentCaptor.forClass(String[].class);
        verify(sender, times(1)).sendAsync(eq("hello"), any(), tagsCaptor.capture(), propertiesCaptor.capture());
        assertEquals(first.getCode(), ChronosSendResult.OK);
        assertEquals(propertiesCaptor.getValue().get("traceId"), "b");
        assertEquals(propertiesCaptor.getValue().get("region"), "hz");
        assertEquals(tagsCaptor.getValue()[0], "tagA");
        assertEquals(tagsCaptor.getValue()[1], "tagB");

        ChronosSendResult second = builder.send();
        assertEquals(second.getCode(), ChronosSendResult.FAIL_UNKNOWN);
    }
}
