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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.ResourceGroups;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.ResourceGroup;


public class ResourceGroupsImpl extends BaseResource implements ResourceGroups {
    private final WebTarget adminResourceGroups;

    public ResourceGroupsImpl(WebTarget web, Authentication auth, long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        adminResourceGroups = web.path("/admin/v2/resourcegroups");
    }

    @Override
    public List<String> getResourceGroups() throws PulsarAdminException {
        return sync(() -> getResourceGroupsAsync());
    }

    @Override
    public CompletableFuture<List<String>> getResourceGroupsAsync() {
        return asyncGetRequest(this.adminResourceGroups, new FutureCallback<List<String>>(){});
    }

    @Override
    public ResourceGroup getResourceGroup(String resourcegroup) throws PulsarAdminException {
        return sync(() -> getResourceGroupAsync(resourcegroup));
    }

    @Override
    public CompletableFuture<ResourceGroup> getResourceGroupAsync(String name) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncGetRequest(path, new FutureCallback<ResourceGroup>(){});
    }

    @Override
    public void createResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException {
        sync(() -> createResourceGroupAsync(name, resourcegroup));
    }

    @Override
    public CompletableFuture<Void> createResourceGroupAsync(String name, ResourceGroup resourcegroup) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncPutRequest(path, Entity.entity(resourcegroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void updateResourceGroup(String name, ResourceGroup resourcegroup) throws PulsarAdminException {
        sync(() -> updateResourceGroupAsync(name, resourcegroup));
    }

    @Override
    public CompletableFuture<Void> updateResourceGroupAsync(String name, ResourceGroup resourcegroup) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncPutRequest(path, Entity.entity(resourcegroup, MediaType.APPLICATION_JSON));
    }

    @Override
    public void deleteResourceGroup(String name) throws PulsarAdminException {
        sync(() -> deleteResourceGroupAsync(name));
    }

    @Override
    public CompletableFuture<Void> deleteResourceGroupAsync(String name) {
        WebTarget path = adminResourceGroups.path(name);
        return asyncDeleteRequest(path);
    }
}
