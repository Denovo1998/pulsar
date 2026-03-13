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

import static org.testng.Assert.expectThrows;
import org.apache.pulsar.client.api.ChronosProducerConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ChronosApiCompatibilityTest {

    @Test
    public void testProducerDefaultChronosMethodsThrowUnsupportedOperationException() {
        Producer<byte[]> producer = Mockito.mock(Producer.class, Mockito.CALLS_REAL_METHODS);

        expectThrows(UnsupportedOperationException.class, producer::newChronosMessage);
        expectThrows(UnsupportedOperationException.class, () -> producer.cancelChronosMessage("id"));
        expectThrows(UnsupportedOperationException.class, () -> producer.cancelChronosMessageAsync("id"));
    }

    @Test
    public void testProducerBuilderDefaultChronosMethodThrowsUnsupportedOperationException() {
        ProducerBuilder<byte[]> builder = Mockito.mock(ProducerBuilder.class, Mockito.CALLS_REAL_METHODS);

        expectThrows(UnsupportedOperationException.class,
                () -> builder.chronosConfiguration(new ChronosProducerConfiguration()
                        .setInnerTopic("persistent://public/default/chronos-inner")));
    }
}
