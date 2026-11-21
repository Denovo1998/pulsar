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
package org.apache.pulsar.broker.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.CustomLog;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.util.Backoff;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.jspecify.annotations.Nullable;

/**
 * Topic policies service backed by Pulsar metadata stores.
 */
@CustomLog
public class MetadataStoreTopicPoliciesService implements TopicPoliciesService {

    public static final String GLOBAL_POLICIES_ROOT = "/admin/topic-policies/global";
    public static final String LOCAL_POLICIES_ROOT = "/admin/topic-policies/local";

    private static final int RECOVERY_BATCH_SIZE = 32;

    // An existing custom backend may implement only the deprecated registration hook.
    private final boolean overridesLegacyRegistration = getClass() != MetadataStoreTopicPoliciesService.class
            && MethodUtils.getMatchingMethod(getClass(), "registerListener", TopicName.class, TopicPolicyListener.class)
                    .getDeclaringClass() != MetadataStoreTopicPoliciesService.class;
    private final CompletableFuture<Void> started = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<TopicName, TopicListeners> listeners = new ConcurrentHashMap<>();
    private PolicyStore local;
    private PolicyStore global;
    private PulsarService pulsar;

    @Override
    public synchronized void start(PulsarService pulsar) {
        if (closed.get() || started.isDone()) {
            return;
        }
        this.pulsar = pulsar;
        MetadataStore localStore = pulsar.getLocalMetadataStore();
        MetadataStore configurationStore = pulsar.getConfigurationMetadataStore();
        local = new PolicyStore(localStore, false);
        global = new PolicyStore(configurationStore, true);
        local.listen(localStore);
        global.listen(configurationStore);
        started.complete(null);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName) {
        return deleteTopicPoliciesAsync(topicName, false);
    }

    @Override
    public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName,
                                                            boolean keepGlobalPoliciesAfterDeleting) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(null);
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        CompletableFuture<Void> deleteLocal =
                deleteIfExists(local.cache, pathFor(partitionedTopicName, false));
        if (keepGlobalPoliciesAfterDeleting) {
            return deleteLocal;
        }
        CompletableFuture<Void> deleteGlobal =
                deleteIfExists(global.cache, pathFor(partitionedTopicName, true));
        return CompletableFuture.allOf(deleteLocal, deleteGlobal);
    }

    @Override
    public CompletableFuture<Void> updateTopicPoliciesAsync(TopicName topicName, boolean isGlobalPolicy,
                                                            boolean skipUpdateWhenTopicPolicyDoesntExist,
                                                            Consumer<TopicPolicies> policyUpdater) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.failedFuture(new BrokerServiceException.NotAllowedException(
                    "Not allowed to update topic policy for the heartbeat topic"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new BrokerServiceException(getClass().getName() + " is closed."));
        }
        MetadataCache<TopicPolicies> cache = cache(isGlobalPolicy);
        String path = pathFor(partitionedTopicName, isGlobalPolicy);
        CompletableFuture<TopicPolicies> updateFuture;
        if (skipUpdateWhenTopicPolicyDoesntExist) {
            updateFuture = cache.readModifyUpdate(path,
                    current -> updatePolicies(Optional.of(current), isGlobalPolicy, policyUpdater));
        } else {
            updateFuture = cache.readModifyUpdateOrCreate(path,
                    current -> updatePolicies(current, isGlobalPolicy, policyUpdater));
        }
        return updateFuture.thenAccept(__ -> { }).exceptionally(error -> {
            if (skipUpdateWhenTopicPolicyDoesntExist
                    && FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    @Override
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesAsync(TopicName topicName, GetType type) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        if (NamespaceService.isHeartbeatNamespace(partitionedTopicName.getNamespaceObject())) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (closed.get()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        boolean global = type == GetType.GLOBAL_ONLY;
        return cache(global).get(pathFor(partitionedTopicName, global))
                .thenApply(policies -> policies.map(policy -> cloneWithScope(policy, global)));
    }

    @Deprecated
    @Override
    public boolean registerListener(TopicName topicName, TopicPolicyListener listener) {
        if (closed.get()) {
            return false;
        }
        addListener(topicName, listener);
        return true;
    }

    @Override
    public CompletableFuture<Boolean> registerListenerAsync(TopicName topicName, TopicPolicyListener listener) {
        if (overridesLegacyRegistration) {
            return TopicPoliciesService.super.registerListenerAsync(topicName, listener);
        }
        return addListener(topicName, listener);
    }

    private CompletableFuture<Boolean> addListener(TopicName topicName, TopicPolicyListener listener) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(false);
        }
        TopicName topic = normalizeTopicName(topicName);
        ListenerRegistration registration = new ListenerRegistration(topic, listener);
        TopicListeners registered = listeners.compute(topic, (__, current) -> {
            if (closed.get()) {
                return current;
            }
            if (current == null) {
                current = new TopicListeners();
            }
            current.entries.add(registration);
            return current;
        });
        if (closed.get() || registered == null) {
            return CompletableFuture.completedFuture(false);
        }
        if (registration.published != null) {
            // Watches must see the registration before its first presence check can reach the store.
            FutureUtil.completeAfter(registration.published, started);
        }
        registration.ready.whenComplete((__, error) -> {
            if (error != null) {
                removeListener(topic, entry -> entry == registration);
            }
        });
        return registration.ready;
    }

    @Override
    public void unregisterListener(TopicName topicName, TopicPolicyListener listener) {
        removeListener(normalizeTopicName(topicName), entry -> entry.listener.equals(listener));
    }

    private void removeListener(TopicName topic, Predicate<ListenerRegistration> match) {
        listeners.computeIfPresent(topic, (__, current) -> {
            for (ListenerRegistration entry : current.entries) {
                if (match.test(entry)) {
                    entry.active = false;
                    current.entries.remove(entry);
                    break;
                }
            }
            return current.entries.isEmpty() ? null : current;
        });
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            started.completeExceptionally(new IllegalStateException("Topic policies service closed before startup"));
            if (local != null) {
                local.close();
            }
            if (global != null) {
                global.close();
            }
            listeners.clear();
        }
    }

    private MetadataCache<TopicPolicies> cache(boolean isGlobalPolicy) {
        return isGlobalPolicy ? global.cache : local.cache;
    }

    private CompletableFuture<Void> deleteIfExists(MetadataCache<TopicPolicies> cache, String path) {
        return cache.delete(path).handle((__, error) -> {
            cache.invalidate(path);
            if (error == null || FutureUtil.unwrapCompletionException(error) instanceof NotFoundException) {
                return null;
            }
            throw FutureUtil.wrapToCompletionException(error);
        });
    }

    private static TopicPolicies updatePolicies(Optional<TopicPolicies> currentPolicies,
                                                boolean isGlobalPolicy,
                                                Consumer<TopicPolicies> policyUpdater) {
        TopicPolicies policies = currentPolicies.map(TopicPolicies::clone).orElseGet(TopicPolicies::new);
        policies.setIsGlobal(isGlobalPolicy);
        policyUpdater.accept(policies);
        return policies;
    }

    private static final class TopicListeners {
        private final List<ListenerRegistration> entries = new CopyOnWriteArrayList<>();
        // A read for a previously unloaded topic cannot supersede a read for its replacement.
        private final AtomicReference<Object> localRequest = new AtomicReference<>();
        private final AtomicReference<Object> globalRequest = new AtomicReference<>();
    }

    private final class ListenerRegistration {
        private final TopicPolicyListener listener;
        private final CompletableFuture<Void> published;
        private final CompletableFuture<Boolean> ready;
        private volatile boolean active = true;
        // Only legacy listeners need presence history. Updates to these futures and all their
        // successful continuations run on the topic's notification thread.
        private CompletableFuture<Boolean> localPresent;
        private CompletableFuture<Boolean> globalPresent;

        ListenerRegistration(TopicName topic, TopicPolicyListener listener) {
            this.listener = listener;
            if (listener instanceof TopicPolicyListener.Scoped) {
                published = null;
                ready = CompletableFuture.completedFuture(true);
            } else {
                published = new CompletableFuture<>();
                Executor executor = task -> pulsar.getBrokerService().getTopicPoliciesNotifyThread(topic).execute(task);
                localPresent = initialPresence(topic, false, executor);
                globalPresent = initialPresence(topic, true, executor);
                ready = localPresent.thenCombine(globalPresent, (localExists, globalExists) -> true);
            }
        }

        private CompletableFuture<Boolean> initialPresence(TopicName topic, boolean global, Executor executor) {
            // Check existence without warming the policy value cache or retaining the policy itself.
            return published.thenCompose(__ -> cache(global).exists(pathFor(topic, global)))
                    .thenApplyAsync(Function.identity(), executor);
        }

        void notifyPolicy(TopicName topic, boolean global, Optional<TopicPolicies> policies, boolean deleted) {
            if (listener instanceof TopicPolicyListener.Scoped) {
                deliver(topic, global, policies, deleted);
                return;
            }
            var previous = global ? globalPresent : localPresent;
            var next = previous.thenApply(present -> {
                deliver(topic, global, policies, deleted || present);
                return policies.isPresent();
            });
            if (global) {
                globalPresent = next;
            } else {
                localPresent = next;
            }
        }

        private void deliver(TopicName topic, boolean global, Optional<TopicPolicies> policies, boolean removed) {
            if (active && !closed.get()) {
                try {
                    if (policies.isPresent()) {
                        listener.onUpdate(policies.get().clone());
                    } else if (listener instanceof TopicPolicyListener.Scoped scoped) {
                        scoped.onPolicyAbsent(global);
                    } else if (removed) {
                        listener.onUpdate(null);
                    }
                } catch (Throwable error) {
                    log.error().attr("topic", topic).exception(error).log("Call topic policy listener error");
                }
            }
        }
    }

    private final class PolicyStore {
        private final boolean globalPolicy;
        private final MetadataCache<TopicPolicies> cache;
        private final FutureUtil.Sequencer<Void> recovery = FutureUtil.Sequencer.create();
        private final AtomicBoolean replayQueued = new AtomicBoolean();
        private Runnable cancelNotifications;
        private Runnable cancelSessionListener;

        PolicyStore(MetadataStore store, boolean globalPolicy) {
            this.globalPolicy = globalPolicy;
            cache = store.getMetadataCache(TopicPolicies.class);
        }

        private void listen(MetadataStore store) {
            cancelNotifications = store.registerCancellableListener(this::handleNotification);
            if (store instanceof MetadataStoreExtended extended) {
                cancelSessionListener = extended.registerCancellableSessionListener(this::handleSessionEvent);
            }
        }

        private void handleSessionEvent(SessionEvent event) {
            if (event.isConnected() && !closed.get() && replayQueued.compareAndSet(false, true)) {
                recovery.sequential(() -> {
                    // A reconnect during this scan needs another pass, even for topics already visited.
                    replayQueued.set(false);
                    var topics = new ArrayList<>(listeners.keySet());
                    CompletableFuture<Void> scan = CompletableFuture.completedFuture(null);
                    for (List<TopicName> batch : Lists.partition(topics, RECOVERY_BATCH_SIZE)) {
                        scan = scan.thenCompose(__ -> FutureUtil.waitForAll(
                                batch.stream().map(topic -> refresh(topic, false, null)).toList()));
                    }
                    return scan;
                });
            }
        }

        private void handleNotification(Notification notification) {
            if (closed.get() || (notification.getType() != NotificationType.Created
                    && notification.getType() != NotificationType.Modified
                    && notification.getType() != NotificationType.Deleted)) {
                return;
            }
            String root = globalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT;
            boolean deleted = notification.getType() == NotificationType.Deleted;
            topicNameFromPath(root, notification.getPath()).ifPresent(topic -> refresh(topic, deleted, null));
        }

        private CompletableFuture<Void> refresh(TopicName topic, boolean deleted, @Nullable Backoff retry) {
            if (closed.get()) {
                return CompletableFuture.completedFuture(null);
            }
            String path = pathFor(topic, globalPolicy);
            cache.invalidate(path);
            TopicListeners topicListeners = listeners.get(topic);
            if (topicListeners == null) {
                return CompletableFuture.completedFuture(null);
            }

            var pending = globalPolicy ? topicListeners.globalRequest : topicListeners.localRequest;
            Object request = new Object();
            pending.set(request);
            // A partition loaded while this read is in flight obtains its own initial policy.
            var recipients = topicListeners.entries.iterator();
            GetType type = globalPolicy ? GetType.GLOBAL_ONLY : GetType.LOCAL_ONLY;
            return getTopicPoliciesAsync(topic, type).handleAsync((policies, error) -> {
                // Only the latest read can notify or schedule a retry for this scope.
                if (closed.get() || listeners.get(topic) != topicListeners) {
                    pending.compareAndSet(request, null);
                } else if (error == null) {
                    if (pending.compareAndSet(request, null)) {
                        recipients.forEachRemaining(entry ->
                                entry.notifyPolicy(topic, globalPolicy, policies, deleted));
                    }
                } else if (pending.get() == request) {
                    log.warn().attr("path", path).exception(error).log("Failed to refresh topic policies");
                    Backoff backoff = retry != null ? retry : Backoff.builder().build();
                    pulsar.getExecutor().schedule(() -> recovery.sequential(() -> {
                        if (pending.compareAndSet(request, null) && listeners.get(topic) == topicListeners) {
                            return refresh(topic, deleted, backoff);
                        }
                        return CompletableFuture.completedFuture(null);
                    }), backoff.next().toMillis(), TimeUnit.MILLISECONDS);
                }
                return null;
            }, pulsar.getBrokerService().getTopicPoliciesNotifyThread(topic));
        }

        private void close() {
            if (cancelNotifications != null) {
                cancelNotifications.run();
            }
            if (cancelSessionListener != null) {
                cancelSessionListener.run();
            }
            cache.close();
        }
    }

    private static TopicName normalizeTopicName(TopicName topicName) {
        return TopicName.get(topicName.getPartitionedTopicName());
    }

    private static TopicPolicies cloneWithScope(TopicPolicies policies, boolean isGlobalPolicy) {
        TopicPolicies cloned = policies.clone();
        cloned.setIsGlobal(isGlobalPolicy);
        return cloned;
    }

    @VisibleForTesting
    public CompletableFuture<Optional<TopicPolicies>> getTopicPoliciesDirectFromStore(TopicName topicName,
                                                                                      boolean isGlobal) {
        String path = pathFor(topicName, isGlobal);
        MetadataCache<TopicPolicies> c = cache(isGlobal);
        c.invalidate(path);
        return c.get(path).thenApply(opt -> opt.map(p -> cloneWithScope(p, isGlobal)));
    }

    @VisibleForTesting
    static String pathFor(TopicName topicName, boolean isGlobalPolicy) {
        TopicName partitionedTopicName = normalizeTopicName(topicName);
        return (isGlobalPolicy ? GLOBAL_POLICIES_ROOT : LOCAL_POLICIES_ROOT)
                + "/" + partitionedTopicName.getTenant()
                + "/" + partitionedTopicName.getNamespacePortion()
                + "/" + partitionedTopicName.getDomain()
                + "/" + partitionedTopicName.getEncodedLocalName();
    }

    @VisibleForTesting
    private static Optional<TopicName> topicNameFromPath(String root, String path) {
        if (!path.startsWith(root + "/")) {
            return Optional.empty();
        }
        String[] parts = path.substring(root.length() + 1).split("/", 4);
        if (parts.length != 4) {
            return Optional.empty();
        }
        return Optional.of(TopicName.get(parts[2], parts[0], parts[1], Codec.decode(parts[3])));
    }
}
