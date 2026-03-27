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
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.AzuriteContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Slf4j
public class TestAzureBlobOffload extends TestBaseOffload {

    private static final String STORAGE_ACCOUNT = "pulsar";
    private static final String STORAGE_KEY_BASE64 = "cHVsc2FyLXRlc3Qta2V5LXB1bHNhci10ZXN0LWtleS0xMjM0";
    private static final String CONTAINER = "pulsar-integtest";
    private static final String AZURE_API_VERSION = "2020-10-02";

    private AzuriteContainer azuriteContainer;

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();

        log.info("Azurite init");
        azuriteContainer = new AzuriteContainer(pulsarCluster.getClusterName(), AzuriteContainer.NAME,
                STORAGE_ACCOUNT, STORAGE_KEY_BASE64)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(AzuriteContainer.NAME);
        azuriteContainer.start();
        createContainerIfMissing();
        log.info("Azurite start finish.");
    }

    @AfterClass(alwaysRun = true)
    public void teardownAzurite() {
        if (azuriteContainer != null) {
            azuriteContainer.stop();
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
        result.put("managedLedgerOffloadDriver", "azureblob");

        // OpenDAL uses `managedLedgerOffloadBucket` as the container name for azblob.
        result.put("managedLedgerOffloadBucket", CONTAINER);
        // Azurite uses path-style endpoint: http://host:port/<accountName>
        result.put("managedLedgerOffloadServiceEndpoint",
                "http://" + AzuriteContainer.NAME + ":" + AzuriteContainer.BLOB_PORT + "/" + STORAGE_ACCOUNT);

        // Keep compatibility with tiered-storage-jcloud behavior (env based credentials).
        result.put("AZURE_STORAGE_ACCOUNT", STORAGE_ACCOUNT);
        result.put("AZURE_STORAGE_ACCESS_KEY", STORAGE_KEY_BASE64);

        return result;
    }

    private void createContainerIfMissing() throws Exception {
        String host = azuriteContainer.getHost();
        int port = azuriteContainer.getMappedPort(AzuriteContainer.BLOB_PORT);

        String date = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC));
        String canonicalizedHeaders = "x-ms-date:" + date + "\n"
                + "x-ms-version:" + AZURE_API_VERSION + "\n";
        String canonicalizedResource = "/" + STORAGE_ACCOUNT + "/" + CONTAINER + "\nrestype:container";

        // See https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
        String stringToSign = "PUT\n" // VERB
                + "\n" // Content-Encoding
                + "\n" // Content-Language
                + "0\n" // Content-Length
                + "\n" // Content-MD5
                + "\n" // Content-Type
                + "\n" // Date (empty because x-ms-date is used)
                + "\n" // If-Modified-Since
                + "\n" // If-Match
                + "\n" // If-None-Match
                + "\n" // If-Unmodified-Since
                + "\n" // Range
                + canonicalizedHeaders
                + canonicalizedResource;

        String signature = buildAzureSignature(STORAGE_KEY_BASE64, stringToSign);
        String authorization = "SharedKey " + STORAGE_ACCOUNT + ":" + signature;

        String uri = "http://" + host + ":" + port + "/" + STORAGE_ACCOUNT + "/" + CONTAINER + "?restype=container";
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .header("x-ms-date", date)
                .header("x-ms-version", AZURE_API_VERSION)
                .header("Authorization", authorization)
                // .header("Content-Length", "0")
                .PUT(HttpRequest.BodyPublishers.ofByteArray(new byte[0]))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        int code = response.statusCode();
        if (code != 201 && code != 202 && code != 409) {
            throw new RuntimeException("Failed to create Azurite container. status=" + code
                    + ", body=" + response.body());
        }
    }

    private static String buildAzureSignature(String base64Key, String stringToSign)
            throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] key = Base64.getDecoder().decode(base64Key);
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        byte[] hmac = mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(hmac);
    }
}

