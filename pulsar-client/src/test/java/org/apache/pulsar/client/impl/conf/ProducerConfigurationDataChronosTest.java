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
package org.apache.pulsar.client.impl.conf;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import org.apache.pulsar.client.api.ChronosProducerConfiguration;
import org.testng.annotations.Test;

public class ProducerConfigurationDataChronosTest {

    @Test
    public void testChronosProducerConfigurationDefaultsAndClone() {
        ChronosProducerConfiguration config = new ChronosProducerConfiguration()
                .setInnerTopic("persistent://public/default/chronos-inner");

        assertEquals(config.getSendTimeoutMs(), ChronosProducerConfiguration.DEFAULT_SEND_TIMEOUT_MS);
        assertEquals(config.isBlockIfQueueFull(), ChronosProducerConfiguration.DEFAULT_BLOCK_IF_QUEUE_FULL);
        assertEquals(config.getMaxPendingMessages(), ChronosProducerConfiguration.DEFAULT_MAX_PENDING_MESSAGES);

        ChronosProducerConfiguration cloned = config.clone();
        cloned.setMaxPendingMessages(99);

        assertNotSame(cloned, config);
        assertEquals(config.getMaxPendingMessages(), ChronosProducerConfiguration.DEFAULT_MAX_PENDING_MESSAGES);
    }

    @Test
    public void testCloneCreatesDefensiveCopyForChronosConfiguration() {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        ChronosProducerConfiguration chronosConfiguration = new ChronosProducerConfiguration()
                .setInnerTopic("persistent://public/default/chronos-inner")
                .setMaxPendingMessages(11);
        conf.setChronosProducerConfiguration(chronosConfiguration);

        ProducerConfigurationData cloned = conf.clone();
        cloned.getChronosProducerConfiguration().setMaxPendingMessages(22);

        assertNotSame(cloned.getChronosProducerConfiguration(), conf.getChronosProducerConfiguration());
        assertEquals(conf.getChronosProducerConfiguration().getMaxPendingMessages(), 11);
    }
}
