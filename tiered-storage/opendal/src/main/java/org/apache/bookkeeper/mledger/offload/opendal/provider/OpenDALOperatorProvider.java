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

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.opendal.Operator;
import org.apache.opendal.ServiceConfig;

@RequiredArgsConstructor
public class OpenDALOperatorProvider {

    private static final String GCS_SERVICE_ACCOUNT_KEY_FILE = "gcsManagedLedgerOffloadServiceAccountKeyFile";
    private static final String ENV_AZURE_STORAGE_ACCOUNT = "AZURE_STORAGE_ACCOUNT";
    private static final String ENV_AZURE_STORAGE_ACCESS_KEY = "AZURE_STORAGE_ACCESS_KEY";

    private final OpenDALTieredStorageConfiguration config;
    private final OperatorCache operatorCache;

    public Operator getOperator(Map<String, String> offloadDriverMetadata) {
        Map<String, String> effective = effectiveLocation(offloadDriverMetadata);
        String scheme = config.getScheme();

        Map<String, String> operatorConfig = new HashMap<>();
        if ("s3".equalsIgnoreCase(scheme)) {
            operatorConfig.putAll(buildS3Config(effective));
        } else if ("gcs".equalsIgnoreCase(scheme)) {
            operatorConfig.putAll(buildGcsConfig(effective));
        } else if ("azblob".equalsIgnoreCase(scheme)) {
            operatorConfig.putAll(buildAzblobConfig(effective));
        } else if ("memory".equalsIgnoreCase(scheme)) {
            operatorConfig.putAll(ServiceConfig.Memory.builder().build().configMap());
        } else {
            throw new IllegalArgumentException("Unsupported OpenDAL scheme: " + scheme);
        }

        // Apply universal extra config passthrough last so that it can override defaults.
        operatorConfig.putAll(config.getExtraConfig());

        OperatorCacheKey cacheKey = OperatorCacheKey.of(
                scheme,
                effective.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET),
                effective.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_REGION),
                effective.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT),
                operatorConfig);

        return operatorCache.getOrCreate(cacheKey, () -> Operator.of(scheme, operatorConfig));
    }

    private Map<String, String> buildS3Config(Map<String, String> effectiveLocation) {
        String bucket = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET);
        String region = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_REGION);
        String endpoint = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT);

        ServiceConfig.S3.S3Builder builder = ServiceConfig.S3.builder().bucket(bucket);

        if (StringUtils.isNotBlank(region)) {
            builder.region(region);
        }
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpoint(endpoint);
        }

        // Align with tiered-storage-jcloud behavior: if a custom endpoint is configured,
        // disable virtual host style by default to avoid DNS issues.
        if (StringUtils.isNotBlank(endpoint) && !config.getExtraConfig().containsKey("enable_virtual_host_style")) {
            builder.enableVirtualHostStyle(false);
        }

        String accessKeyId = config.getConfigProperties().get("s3ManagedLedgerOffloadCredentialId");
        String secretAccessKey = config.getConfigProperties().get("s3ManagedLedgerOffloadCredentialSecret");
        boolean hasExplicitCredentials = StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretAccessKey);
        if (hasExplicitCredentials) {
            builder.accessKeyId(accessKeyId).secretAccessKey(secretAccessKey);
        }

        String roleArn = config.getConfigProperties().get("s3ManagedLedgerOffloadRole");
        boolean hasRole = StringUtils.isNotBlank(roleArn);
        if (StringUtils.isNotBlank(roleArn)) {
            builder.roleArn(roleArn);
        }
        String roleSessionName = config.getConfigProperties().get("s3ManagedLedgerOffloadRoleSessionName");
        if (StringUtils.isNotBlank(roleSessionName)) {
            builder.roleSessionName(roleSessionName);
        }

        // Keep behavior compatible with the existing S3 integration tests that use a mock endpoint without credentials.
        // IMPORTANT: do NOT default to anonymous for real AWS S3 (endpoint not explicitly set).
        String driver = config.getDriver();
        boolean hasEnvAwsCredentials = StringUtils.isNotBlank(System.getenv("AWS_ACCESS_KEY_ID"))
                && StringUtils.isNotBlank(System.getenv("AWS_SECRET_ACCESS_KEY"));
        boolean shouldDefaultAllowAnonymous = "aws-s3".equalsIgnoreCase(driver)
                && StringUtils.isNotBlank(endpoint)
                && !hasExplicitCredentials
                && !hasRole
                && !hasEnvAwsCredentials
                && !config.getExtraConfig().containsKey("allow_anonymous");
        if (shouldDefaultAllowAnonymous) {
            builder.allowAnonymous(true);
        }

        return builder.build().configMap();
    }

    private Map<String, String> buildGcsConfig(Map<String, String> effectiveLocation) {
        String bucket = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET);
        String endpoint = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT);

        ServiceConfig.Gcs.GcsBuilder builder = ServiceConfig.Gcs.builder().bucket(bucket);
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpoint(endpoint);
        }

        // Keep compatibility with tiered-storage-jcloud: allow providing the service account key via file path.
        // If users explicitly pass credential settings via managedLedgerOffloadExtraConfig*, prefer those.
        Map<String, String> extra = config.getExtraConfig();
        boolean hasExplicitCredential = extra.containsKey("credential")
                || extra.containsKey("credential_path")
                || extra.containsKey("service_account")
                || extra.containsKey("token");
        boolean isHttpEndpoint = endpoint != null && endpoint.regionMatches(true, 0, "http://", 0, "http://".length());
        String keyFilePath = StringUtils.trimToNull(config.getConfigProperties().get(GCS_SERVICE_ACCOUNT_KEY_FILE));
        if (!hasExplicitCredential) {
            if (keyFilePath != null) {
                builder.credentialPath(keyFilePath);
            } else if (isHttpEndpoint) {
                // Emulator-friendly defaults:
                // - Fake GCS server typically does not require auth.
                // - Disable local config load / VM metadata to avoid slow fallbacks when running in containers.
                if (!extra.containsKey("allow_anonymous")) {
                    builder.allowAnonymous(true);
                }
                if (!extra.containsKey("disable_config_load")) {
                    builder.disableConfigLoad(true);
                }
                if (!extra.containsKey("disable_vm_metadata")) {
                    builder.disableVmMetadata(true);
                }
            }
        }
        return builder.build().configMap();
    }

    private Map<String, String> buildAzblobConfig(Map<String, String> effectiveLocation) {
        // In OpenDAL, azblob uses "container" instead of "bucket".
        String container = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET);
        String endpoint = effectiveLocation.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT);

        ServiceConfig.Azblob.AzblobBuilder builder = ServiceConfig.Azblob.builder().container(container);
        if (StringUtils.isNotBlank(endpoint)) {
            builder.endpoint(endpoint);
        }

        // Keep compatibility with tiered-storage-jcloud: by default Azure credentials are sourced from env vars.
        // Users can override by passing managedLedgerOffloadExtraConfigaccountName/accountKey/sasToken, etc.
        Map<String, String> extra = config.getExtraConfig();
        boolean hasAccountName = extra.containsKey("account_name");
        boolean hasAccountKey = extra.containsKey("account_key");
        boolean hasSasToken = extra.containsKey("sas_token");

        String accountName = StringUtils.trimToNull(System.getenv(ENV_AZURE_STORAGE_ACCOUNT));
        String accountKey = StringUtils.trimToNull(System.getenv(ENV_AZURE_STORAGE_ACCESS_KEY));
        if (!hasAccountName && accountName != null) {
            builder.accountName(accountName);
        }
        // Don't apply account key when SAS auth is explicitly configured.
        if (!hasSasToken && !hasAccountKey && accountKey != null) {
            builder.accountKey(accountKey);
        }
        return builder.build().configMap();
    }

    private Map<String, String> effectiveLocation(Map<String, String> offloadDriverMetadata) {
        Map<String, String> effective = new HashMap<>();

        effective.put(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET,
                firstNonBlank(offloadDriverMetadata.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_BUCKET),
                        config.getBucket()));
        effective.put(OpenDALTieredStorageConfiguration.METADATA_FIELD_REGION,
                firstNonBlank(offloadDriverMetadata.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_REGION),
                        config.getRegion()));
        effective.put(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT,
                firstNonBlank(offloadDriverMetadata.get(OpenDALTieredStorageConfiguration.METADATA_FIELD_ENDPOINT),
                        config.getServiceEndpoint()));

        return effective;
    }

    private static String firstNonBlank(String first, String second) {
        if (StringUtils.isNotBlank(first)) {
            return first;
        }
        return StringUtils.defaultString(second);
    }
}
