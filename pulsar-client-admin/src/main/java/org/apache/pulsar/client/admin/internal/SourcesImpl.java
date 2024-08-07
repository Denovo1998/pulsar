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
package org.apache.pulsar.client.admin.internal;

import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;
import com.google.gson.Gson;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Source;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.admin.internal.http.AsyncHttpRequestExecutor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Slf4j
public class SourcesImpl extends ComponentResource implements Sources, Source {

    private final WebTarget source;
    private final AsyncHttpRequestExecutor asyncHttpRequestExecutor;

    public SourcesImpl(WebTarget web, Authentication auth, AsyncHttpRequestExecutor asyncHttpRequestExecutor,
                       long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        this.source = web.path("/admin/v3/source");
        this.asyncHttpRequestExecutor = asyncHttpRequestExecutor;
    }

    @Override
    public List<String> listSources(String tenant, String namespace) throws PulsarAdminException {
        return sync(() -> listSourcesAsync(tenant, namespace));
    }

    @Override
    public CompletableFuture<List<String>> listSourcesAsync(String tenant, String namespace) {
        WebTarget path = source.path(tenant).path(namespace);
        return asyncGetRequest(path, new GenericType<List<String>>() {});
    }

    @Override
    public SourceConfig getSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        return sync(() -> getSourceAsync(tenant, namespace, sourceName));
    }

    @Override
    public CompletableFuture<SourceConfig> getSourceAsync(String tenant, String namespace, String sourceName) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName);
        return asyncGetRequest(path, SourceConfig.class);
    }

    @Override
    public SourceStatus getSourceStatus(
            String tenant, String namespace, String sourceName) throws PulsarAdminException {
        return sync(() -> getSourceStatusAsync(tenant, namespace, sourceName));
    }

    @Override
    public CompletableFuture<SourceStatus> getSourceStatusAsync(String tenant, String namespace, String sourceName) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path("status");
        return asyncGetRequest(path, SourceStatus.class);
    }

    @Override
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceStatus(
            String tenant, String namespace, String sourceName, int id) throws PulsarAdminException {
        return sync(() -> getSourceStatusAsync(tenant, namespace, sourceName, id));
    }

    @Override
    public CompletableFuture<SourceStatus.SourceInstanceStatus.SourceInstanceStatusData> getSourceStatusAsync(
            String tenant, String namespace, String sourceName, int id) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(id)).path("status");
        return asyncGetRequest(path, SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class);
    }

    @Override
    public void createSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException {
        sync(() -> createSourceAsync(sourceConfig, fileName));
    }

    @Override
    public CompletableFuture<Void> createSourceAsync(SourceConfig sourceConfig, String fileName) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    post(source.path(sourceConfig.getTenant())
                            .path(sourceConfig.getNamespace()).path(sourceConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sourceConfig", objectWriter()
                            .writeValueAsString(sourceConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpRequestExecutor.executeRequest(addAuthHeaders(source, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
                        } else {
                            future.complete(null);
                        }
                    })
                    .exceptionally(throwable -> {
                        future.completeExceptionally(getApiException(throwable));
                        return null;
                    });
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void createSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException {
        sync(() -> createSourceWithUrlAsync(sourceConfig, pkgUrl));
    }

    @Override
    public CompletableFuture<Void> createSourceWithUrlAsync(SourceConfig sourceConfig, String pkgUrl) {
        final FormDataMultiPart mp = new FormDataMultiPart();
        mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
        mp.bodyPart(new FormDataBodyPart("sourceConfig",
                new Gson().toJson(sourceConfig),
                MediaType.APPLICATION_JSON_TYPE));
        WebTarget path = source.path(sourceConfig.getTenant())
                .path(sourceConfig.getNamespace()).path(sourceConfig.getName());
        return asyncPostRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
    }

    @Override
    public void deleteSource(String cluster, String namespace, String function) throws PulsarAdminException {
        sync(() -> deleteSourceAsync(cluster, namespace, function));
    }

    @Override
    public CompletableFuture<Void> deleteSourceAsync(String tenant, String namespace, String function) {
        WebTarget path = source.path(tenant).path(namespace).path(function);
        return asyncDeleteRequest(path);
    }

    @Override
    public void updateSource(SourceConfig sourceConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException {
        sync(() -> updateSourceAsync(sourceConfig, fileName, updateOptions));
    }

    @Override
    public CompletableFuture<Void> updateSourceAsync(
            SourceConfig sourceConfig, String fileName, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    put(source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace())
                            .path(sourceConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sourceConfig", objectWriter()
                            .writeValueAsString(sourceConfig), MediaType.APPLICATION_JSON));

            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                builder.addBodyPart(new StringPart("updateOptions",
                        objectWriter().writeValueAsString(options),
                        MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpRequestExecutor.executeRequest(addAuthHeaders(source, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
                        } else {
                            future.complete(null);
                        }
                    })
                    .exceptionally(throwable -> {
                        future.completeExceptionally(getApiException(throwable));
                        return null;
                    });
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException {
        updateSource(sourceConfig, fileName, null);
    }

    @Override
    public CompletableFuture<Void> updateSourceAsync(SourceConfig sourceConfig, String fileName) {
        return updateSourceAsync(sourceConfig, fileName, null);
    }

    @Override
    public void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException {
        sync(() -> updateSourceWithUrlAsync(sourceConfig, pkgUrl, updateOptions));
    }

    @Override
    public CompletableFuture<Void> updateSourceWithUrlAsync(
            SourceConfig sourceConfig, String pkgUrl, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
            mp.bodyPart(new FormDataBodyPart(
                    "sourceConfig",
                    new Gson().toJson(sourceConfig),
                    MediaType.APPLICATION_JSON_TYPE));
            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        objectWriter().writeValueAsString(options),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            WebTarget path = source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace())
                    .path(sourceConfig.getName());
            return asyncPutRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException {
        updateSourceWithUrl(sourceConfig, pkgUrl, null);
    }

    @Override
    public CompletableFuture<Void> updateSourceWithUrlAsync(SourceConfig sourceConfig, String pkgUrl) {
        return updateSourceWithUrlAsync(sourceConfig, pkgUrl, null);
    }

    @Override
    public void restartSource(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        sync(() -> restartSourceAsync(tenant, namespace, functionName, instanceId));
    }

    @Override
    public CompletableFuture<Void> restartSourceAsync(
            String tenant, String namespace, String functionName, int instanceId) {
        WebTarget path = source.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                .path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void restartSource(String tenant, String namespace, String functionName) throws PulsarAdminException {
        sync(() -> restartSourceAsync(tenant, namespace, functionName));
    }

    @Override
    public CompletableFuture<Void> restartSourceAsync(String tenant, String namespace, String functionName) {
        WebTarget path = source.path(tenant).path(namespace).path(functionName).path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopSource(String tenant, String namespace, String sourceName, int instanceId)
            throws PulsarAdminException {
        sync(() -> stopSourceAsync(tenant, namespace, sourceName, instanceId));
    }

    @Override
    public CompletableFuture<Void> stopSourceAsync(String tenant, String namespace, String sourceName, int instanceId) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(instanceId))
                .path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        sync(() -> stopSourceAsync(tenant, namespace, sourceName));
    }

    @Override
    public CompletableFuture<Void> stopSourceAsync(String tenant, String namespace, String sourceName) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startSource(String tenant, String namespace, String sourceName, int instanceId)
            throws PulsarAdminException {
        sync(() -> startSourceAsync(tenant, namespace, sourceName, instanceId));
    }

    @Override
    public CompletableFuture<Void> startSourceAsync(
            String tenant, String namespace, String sourceName, int instanceId) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(instanceId))
                .path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        sync(() -> startSourceAsync(tenant, namespace, sourceName));
    }

    @Override
    public CompletableFuture<Void> startSourceAsync(String tenant, String namespace, String sourceName) {
        WebTarget path = source.path(tenant).path(namespace).path(sourceName).path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<ConnectorDefinition> getBuiltInSources() throws PulsarAdminException {
        return sync(() -> getBuiltInSourcesAsync());
    }

    @Override
    public CompletableFuture<List<ConnectorDefinition>> getBuiltInSourcesAsync() {
        WebTarget path = source.path("builtinsources");
        final CompletableFuture<List<ConnectorDefinition>> future = new CompletableFuture<>();
        return asyncGetRequest(path, new GenericType<List<ConnectorDefinition>>() {});
    }

    @Override
    public void reloadBuiltInSources() throws PulsarAdminException {
        sync(() -> reloadBuiltInSourcesAsync());
    }

    @Override
    public CompletableFuture<Void> reloadBuiltInSourcesAsync() {
        WebTarget path = source.path("reloadBuiltInSources");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }
}
