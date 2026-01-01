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
package org.apache.pulsar.tests.integration.offload;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.GcsContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Slf4j
public class TestGcsOffload extends TestBaseOffload {

    private static final String BUCKET = "pulsar-integtest";
    private GcsContainer gcsContainer;

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();

        log.info("GCS emulator init");
        gcsContainer = new GcsContainer(pulsarCluster.getClusterName(), GcsContainer.NAME)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(GcsContainer.NAME);
        gcsContainer.start();
        createBucketIfMissing();
        log.info("GCS emulator start finish.");
    }

    @AfterClass(alwaysRun = true)
    public void teardownGcs() {
        if (gcsContainer != null) {
            gcsContainer.stop();
        }
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaCLI(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testPublishOffloadAndConsumeViaCLI(serviceUrl.get(), adminUrl.get());
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaThreshold(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testPublishOffloadAndConsumeViaThreshold(serviceUrl.get(), adminUrl.get());
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeDeletionLag(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testPublishOffloadAndConsumeDeletionLag(serviceUrl.get(), adminUrl.get());
    }

    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(getNumEntriesPerLedger()));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "google-cloud-storage");

        // Keep compatibility with legacy GCS keys.
        result.put("gcsManagedLedgerOffloadBucket", BUCKET);
        result.put("gcsManagedLedgerOffloadServiceEndpoint",
                "http://" + GcsContainer.NAME + ":" + GcsContainer.PORT);

        // Emulator-friendly settings. All of these are passed through to OpenDAL via extra config.
        result.put("managedLedgerOffloadExtraConfigallowAnonymous", "true");
        result.put("managedLedgerOffloadExtraConfigdisableConfigLoad", "true");
        result.put("managedLedgerOffloadExtraConfigdisableVmMetadata", "true");

        return result;
    }

    private void createBucketIfMissing() throws Exception {
        String host = gcsContainer.getHost();
        int port = gcsContainer.getMappedPort(GcsContainer.PORT);

        HttpClient client = HttpClient.newHttpClient();
        String uri = "http://" + host + ":" + port + "/storage/v1/b?project=pulsar-it";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"" + BUCKET + "\"}"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        int code = response.statusCode();
        if (code != 200 && code != 409) {
            throw new RuntimeException("Failed to create GCS bucket via emulator. status=" + code
                    + ", body=" + response.body());
        }
    }
}

