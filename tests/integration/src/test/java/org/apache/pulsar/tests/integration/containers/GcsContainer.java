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
package org.apache.pulsar.tests.integration.containers;

import lombok.extern.slf4j.Slf4j;

/**
 * Google Cloud Storage emulator container.
 */
@Slf4j
public class GcsContainer extends ChaosContainer<GcsContainer> {

    public static final String NAME = "gcs";
    public static final int PORT = 4443;
    private static final String IMAGE_NAME = "fsouza/fake-gcs-server:latest";

    private final String hostname;

    public GcsContainer(String clusterName, String hostname) {
        super(clusterName, IMAGE_NAME);
        this.hostname = hostname;
        this.withExposedPorts(PORT);
        // Use HTTP to avoid TLS certificate issues inside the Pulsar broker container.
        this.withCommand("-scheme", "http", "-backend", "memory");
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + hostname;
    }

    @Override
    public void start() {
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName(hostname);
            createContainerCmd.withName(getContainerName());
        });

        super.start();
        log.info("Start GCS emulator service");
    }
}

