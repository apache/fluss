/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.authorizer;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.utils.FatalErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ZkData.AclChangeNotificationNode;
import org.apache.fluss.server.zk.data.ZkData.AclChangesNode;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Maps;
import org.apache.fluss.shaded.guava32.com.google.common.collect.Sets;
import org.apache.fluss.utils.clock.SystemClock;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.fluss.security.acl.Resource.TABLE_SPLITTER;
import static org.apache.fluss.server.zk.ZooKeeperClient.UNKNOWN_VERSION;

/**
 * A default authorization manager that leverages ZooKeeper to store access control lists (ACLs).
 */
public class DefaultAuthorizer extends AbstractAuthorizer implements FatalErrorHandler {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultAuthorizer.class);

    /**
     * Static mapping of ResourceType to the set of resources it contains. This defines the
     * hierarchical relationship between resource types.
     */
    private static final Map<ResourceType, Function<Resource, Set<Resource>>> RESOURCE_MAPPING;

    /**
     * Static mapping of OperationType to the set of operations it includes. This defines the
     * inheritance relationship between operation types.
     */
    private static final Map<OperationType, Set<OperationType>> OPS_MAPPING;

    // The maximum number of times we should try to update the resource acls in zookeeper before
    // failing;
    // This should never occur, but is a safeguard just in case.
    private static final int MAX_UPDATE_RETRIES = 10;
    private static final int INIT_RETRY_BACKOFF_MS = 100;
    private static final int RETRY_BACKOFF_JITTER_MS = 50;
    private static final boolean SHOULD_ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND = false;

    static {
        Map<ResourceType, Function<Resource, Set<Resource>>> mapping =
                new EnumMap<>(ResourceType.class);
        mapping.put(
                ResourceType.TABLE,
                res -> {
                    String[] split = res.getName().split(TABLE_SPLITTER);
                    return Sets.newHashSet(res, Resource.database(split[0]), Resource.cluster());
                });
        mapping.put(ResourceType.DATABASE, res -> Sets.newHashSet(res, Resource.cluster()));
        mapping.put(ResourceType.CLUSTER, Sets::newHashSet);
        RESOURCE_MAPPING = Collections.unmodifiableMap(mapping);

        Map<OperationType, Set<OperationType>> map = new EnumMap<>(OperationType.class);
        map.put(
                OperationType.DESCRIBE,
                Sets.newHashSet(
                        OperationType.DESCRIBE,
                        OperationType.READ,
                        OperationType.WRITE,
                        OperationType.CREATE,
                        OperationType.DROP,
                        OperationType.ALTER));
        OPS_MAPPING = Collections.unmodifiableMap(map);
    }

    private final Set<FlussPrincipal> superUsers;

    private final ZooKeeperClient zooKeeperClient;
    private final ZkNodeChangeNotificationWatcher aclChangeNotificationWatcher;
    private final Object lock = new Object();

    // Main cache: Stores the mapping between resources and access control entries, sorted by
    // resource.
    private final TreeMap<Resource, VersionedAcls> aclCache = new TreeMap<>(new ResourceOrdering());

    // Reverse index cache: Maps access control entry types to resources for quick lookups.
    private final HashMap<ResourceTypeKey, Set<String>> resourceCache = new HashMap<>();

    public DefaultAuthorizer(AuthorizationPlugin.Context context) {
        Configuration configuration = context.getConfiguration();
        this.superUsers = parseSuperUsers(configuration);
        if (context.getZooKeeperClient().isPresent()) {
            this.zooKeeperClient = context.getZooKeeperClient().get();
        } else {
            this.zooKeeperClient = ZooKeeperUtils.startZookeeperClient(configuration, this);
        }
        this.aclChangeNotificationWatcher =
                new ZkNodeChangeNotificationWatcher(
                        zooKeeperClient,
                        AclChangesNode.path(),
                        AclChangeNotificationNode.prefix(),
                        configuration
                                .get(ConfigOptions.ACL_NOTIFICATION_EXPIRATION_TIME)
                                .toMillis(),
                        new ZkNotificationHandler(),
                        SystemClock.getInstance());
    }

    @Override
    public void startup() throws Exception {
        aclChangeNotificationWatcher.start();
        loadCache();
    }

    @Override
    public void close() {
        if (aclChangeNotificationWatcher != null) {
            aclChangeNotificationWatcher.stop();
        }
    }

    @Override
    public boolean authorizeAction(Session session, Action action) {
        FlussPrincipal principal = session.getPrincipal();
        return superUsers.contains(principal)
                || aclsAllowAccess(
                        action.getResource(),
                        principal,
                        action.getOperation(),
                        session.getInetAddress().getHostAddress());
    }

    @Override
    public List<AclCreateResult> addAcls(Session session, List<AclBinding> aclBindings) {
        if (aclBindings.isEmpty()) {
            return Collections.emptyList();
        }
        AclCreateResult[] results = new AclCreateResult[aclBindings.size()];
        // key is resource, while is the index of acl binding in aclBindings.
        Map<Resource, Map<AccessControlEntry, Integer>> aclsToCreate =
                groupAclsByResource(aclBindings);
        synchronized (lock) {
            authorizeAclOperation(session, aclsToCreate.keySet());

            aclsToCreate.forEach(
                    (resource, entries) -> {
                        try {
                            updateResourceAcl(
                                    resource,
                                    (currentAcls) -> {
                                        Set<AccessControlEntry> newAcls =
                                                new HashSet<>(currentAcls);
                                        newAcls.addAll(entries.keySet());
                                        return newAcls;
                                    });
                            entries.values()
                                    .forEach(
                                            idx ->
                                                    results[idx] =
                                                            AclCreateResult.success(
                                                                    aclBindings.get(idx)));

                        } catch (Throwable e) {
                            ApiException exception = ApiError.fromThrowable(e).exception();
                            entries.values()
                                    .forEach(
                                            idx ->
                                                    results[idx] =
                                                            new AclCreateResult(
                                                                    aclBindings.get(idx),
                                                                    exception));
                        }
                    });
        }
        return Arrays.asList(results);
    }

    @Override
    public List<AclDeleteResult> dropAcls(
            Session session, List<AclBindingFilter> aclBindingFilters) {
        Map<AclBinding, Integer> deletedBindings = new HashMap<>();
        Map<AclBinding, ApiError> deleteExceptions = new HashMap<>();
        List<Tuple2<AclBindingFilter, Integer>> filters =
                IntStream.range(0, aclBindingFilters.size())
                        .mapToObj(i -> Tuple2.of(aclBindingFilters.get(i), i))
                        .collect(Collectors.toList());

        synchronized (lock) {
            Set<Resource> resources = new HashSet<>(aclCache.keySet());
            Map<Resource, List<Tuple2<AclBindingFilter, Integer>>> resourcesToUpdate =
                    new HashMap<>();
            for (Resource resource : resources) {
                List<Tuple2<AclBindingFilter, Integer>> matchingFilters = new ArrayList<>();
                for (Tuple2<AclBindingFilter, Integer> filter : filters) {
                    if (filter.f0.getResourceFilter().matches(resource)) {
                        matchingFilters.add(filter);
                    }
                }
                if (!matchingFilters.isEmpty()) {
                    resourcesToUpdate.put(resource, matchingFilters);
                }
            }

            authorizeAclOperation(session, resourcesToUpdate.keySet());
            for (Map.Entry<Resource, List<Tuple2<AclBindingFilter, Integer>>> entry :
                    resourcesToUpdate.entrySet()) {
                Resource resource = entry.getKey();
                List<Tuple2<AclBindingFilter, Integer>> matchingFilters = entry.getValue();
                Map<AclBinding, Integer> resourceBindingsBeingDeleted = new HashMap<>();

                try {
                    updateResourceAcl(
                            resource,
                            currentAcls -> {
                                Set<AccessControlEntry> aclsToRemove = new HashSet<>();
                                for (AccessControlEntry acl : currentAcls) {
                                    for (Tuple2<AclBindingFilter, Integer> filter :
                                            matchingFilters) {
                                        if (filter.f0.getEntryFilter().matches(acl)) {
                                            AclBinding binding = new AclBinding(resource, acl);
                                            deletedBindings.putIfAbsent(binding, filter.f1);
                                            resourceBindingsBeingDeleted.putIfAbsent(
                                                    binding, filter.f1);
                                            aclsToRemove.add(acl);
                                        }
                                    }
                                }
                                return Sets.difference(currentAcls, aclsToRemove);
                            });
                } catch (Exception e) {
                    for (AclBinding binding : resourceBindingsBeingDeleted.keySet()) {
                        ApiError apiError = ApiError.fromThrowable(e);
                        deleteExceptions.putIfAbsent(binding, apiError);
                    }
                }
            }
        }

        Map<Integer, Set<AclDeleteResult.AclBindingDeleteResult>> deletedResult = new HashMap<>();
        for (Map.Entry<AclBinding, Integer> entry : deletedBindings.entrySet()) {
            deletedResult
                    .computeIfAbsent(entry.getValue(), k -> new HashSet<>())
                    .add(
                            new AclDeleteResult.AclBindingDeleteResult(
                                    entry.getKey(),
                                    deleteExceptions.getOrDefault(entry.getKey(), null)));
        }

        List<AclDeleteResult> results = new ArrayList<>();
        for (int i = 0; i < aclBindingFilters.size(); i++) {
            Set<AclDeleteResult.AclBindingDeleteResult> bindings =
                    deletedResult.getOrDefault(i, Collections.emptySet());
            results.add(new AclDeleteResult(bindings));
        }

        return results;
    }

    @Override
    public Collection<AclBinding> listAcls(Session session, AclBindingFilter aclBindingFilter) {
        Set<AclBinding> aclBindings = new HashSet<>();

        aclCache.forEach(
                (resource, aclSet) -> {
                    if (isAuthorized(session, OperationType.DESCRIBE, resource)) {
                        aclSet.acls.forEach(
                                acl -> {
                                    AclBinding aclBinding = new AclBinding(resource, acl);
                                    if (aclBindingFilter.matches(aclBinding)) {
                                        aclBindings.add(aclBinding);
                                    }
                                });
                    }
                });

        return aclBindings;
    }

    private void loadCache() throws Exception {
        synchronized (lock) {
            ResourceType[] resourceTypes = ResourceType.values();
            for (ResourceType resourceType : resourceTypes) {
                List<String> resourceNames = zooKeeperClient.listResourcesByType(resourceType);
                for (String resourceName : resourceNames) {
                    Resource resource = new Resource(resourceType, resourceName);
                    VersionedAcls versionedAcls = getAclsFromZk(resource);
                    updateCache(resource, versionedAcls);
                }
            }
        }
    }

    private Map<Resource, Map<AccessControlEntry, Integer>> groupAclsByResource(
            List<AclBinding> aclBindings) {
        List<Map.Entry<AclBinding, Integer>> aclBindingsWithIndex = new ArrayList<>();
        for (int i = 0; i < aclBindings.size(); i++) {
            aclBindingsWithIndex.add(Maps.immutableEntry(aclBindings.get(i), i));
        }

        return aclBindingsWithIndex.stream()
                .collect(
                        Collectors.groupingBy(
                                entry -> entry.getKey().getResource(),
                                Collectors.toMap(
                                        aclBindingIntegerEntry ->
                                                aclBindingIntegerEntry
                                                        .getKey()
                                                        .getAccessControlEntry(),
                                        Map.Entry::getValue)));
    }

    private void updateResourceAcl(
            Resource resource,
            Function<Set<AccessControlEntry>, Set<AccessControlEntry>> newAclSupplier)
            throws Exception {
        boolean writeComplete = false;
        int retries = 0;
        Throwable lastException = null;

        VersionedAcls currentVersionedAcls =
                aclCache.containsKey(resource)
                        ? getAclsFromCache(resource)
                        : getAclsFromZk(resource);
        VersionedAcls newVersionedAcls = null;
        Set<AccessControlEntry> newAces;
        long backoffMs = INIT_RETRY_BACKOFF_MS;
        while (!writeComplete && retries <= MAX_UPDATE_RETRIES) {
            newAces = newAclSupplier.apply(currentVersionedAcls.acls);
            try {
                int updateVersion = 0;
                if (!newAces.isEmpty()) {
                    if (currentVersionedAcls.exists()) {
                        updateVersion =
                                zooKeeperClient.updateResourceAcl(
                                        resource, newAces, currentVersionedAcls.zkVersion);
                    } else {
                        zooKeeperClient.createResourceAcl(resource, newAces);
                    }

                } else {
                    LOG.trace("Deleting path for {} because it had no ACLs remaining", resource);
                    zooKeeperClient.conditionalDeleteResourceAcl(
                            resource, currentVersionedAcls.zkVersion);
                }
                writeComplete = true;
                newVersionedAcls = new VersionedAcls(updateVersion, newAces);
            } catch (Throwable e) {
                LOG.error(
                        "Failed to update ACLs for {} after trying a of {} times. Retry again.",
                        resource,
                        retries,
                        e);
                Thread.sleep(backoffMs);
                backoffMs = backoffTime(backoffMs);
                currentVersionedAcls = getAclsFromZk(resource);
                retries++;
                lastException = e;
            }
        }

        if (!writeComplete) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to update ACLs for %s after trying a maximum of %s times, last exception is ",
                            resource, MAX_UPDATE_RETRIES),
                    lastException);
        }

        assert newVersionedAcls != null;
        if (!newVersionedAcls.acls.equals(currentVersionedAcls.acls)) {
            updateCache(resource, newVersionedAcls);
            updateAclChangedFlag(resource);
        } else {
            LOG.debug("Updated ACLs for {}, no change was made", resource);
            // Even if no change, update the version
            updateCache(resource, newVersionedAcls);
        }
    }

    private void updateCache(Resource resource, VersionedAcls versionedAcls) {
        Set<AccessControlEntry> currentAces =
                aclCache.containsKey(resource) ? aclCache.get(resource).acls : new HashSet<>();
        Set<AccessControlEntry> acesToAdd = new HashSet<>(versionedAcls.acls);
        acesToAdd.removeAll(currentAces);
        Set<AccessControlEntry> acesToRemove = new HashSet<>(currentAces);
        acesToRemove.removeAll(versionedAcls.acls);

        acesToAdd.forEach(
                ace -> {
                    ResourceTypeKey resourceTypeKey = new ResourceTypeKey(ace, resource.getType());
                    resourceCache
                            .computeIfAbsent(resourceTypeKey, k -> new HashSet<>())
                            .add(resource.getName());
                });

        acesToRemove.forEach(
                ace -> {
                    ResourceTypeKey resourceTypeKey = new ResourceTypeKey(ace, resource.getType());
                    resourceCache.computeIfPresent(
                            resourceTypeKey,
                            (k, v) -> {
                                v.remove(resource.getName());
                                return v.isEmpty() ? null : v;
                            });
                });

        if (versionedAcls.acls.isEmpty()) {
            aclCache.remove(resource);
        } else {
            aclCache.put(resource, versionedAcls);
        }
    }

    private void updateAclChangedFlag(Resource resource) {
        try {
            zooKeeperClient.insertAclChangeNotification(resource);
        } catch (Exception e) {
            LOG.error("Failed to update acl change flag for {}", resource, e);
            throw new IllegalStateException(
                    String.format("Failed to update acl change flag for %s", resource), e);
        }
    }

    @VisibleForTesting
    public boolean aclsAllowAccess(
            Resource resource, FlussPrincipal principal, OperationType operation, String host) {
        Set<AccessControlEntry> accessControlEntries = matchingAcls(resource);
        return isEmptyAclAndAuthorized(resource, accessControlEntries)
                || allowAclExists(resource, principal, operation, host, accessControlEntries);
    }

    private boolean isEmptyAclAndAuthorized(Resource resource, Collection acls) {
        if (acls.isEmpty()) {
            LOG.debug(
                    "No acl found for resource {}, authorized = {}",
                    resource,
                    SHOULD_ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND);
            return SHOULD_ALLOW_EVERYONE_IF_NO_ACL_IS_FOUND;
        }
        return false;
    }

    private boolean allowAclExists(
            Resource resource,
            FlussPrincipal principal,
            OperationType operation,
            String host,
            Set<AccessControlEntry> acls) {

        Set<OperationType> allowOps =
                OPS_MAPPING.getOrDefault(operation, Collections.singleton(operation));
        for (OperationType allowOp : allowOps) {
            if (matchingAclExists(allowOp, resource, principal, host, PermissionType.ALLOW, acls)) {
                return true;
            }
        }

        return false;
    }

    private boolean matchingAclExists(
            OperationType operation,
            Resource resource,
            FlussPrincipal principal,
            String host,
            PermissionType permissionType,
            Set<AccessControlEntry> acls) {
        return acls.stream()
                .filter(
                        acl ->
                                acl.getPermissionType() == permissionType
                                        && (acl.getPrincipal().equals(principal)
                                                || acl.getPrincipal()
                                                        .equals(FlussPrincipal.WILD_CARD_PRINCIPAL))
                                        && (operation == acl.getOperationType()
                                                || acl.getOperationType() == OperationType.ALL)
                                        && (acl.getHost().equals(AccessControlEntry.WILD_CARD_HOST)
                                                || acl.getHost().equals(host)))
                .findFirst()
                .map(
                        acl -> {
                            LOG.debug(
                                    "operation = {} on resource = {} from host = {} is {} based on acl = {}",
                                    operation,
                                    resource,
                                    host,
                                    permissionType,
                                    acl);
                            return true;
                        })
                .orElse(false);
    }

    private Set<AccessControlEntry> matchingAcls(Resource resource) {
        TreeMap<Resource, VersionedAcls> aclCacheSnapshot = aclCache;
        Set<AccessControlEntry> wildcard =
                Optional.ofNullable(
                                aclCacheSnapshot.get(
                                        new Resource(
                                                resource.getType(), Resource.WILDCARD_RESOURCE)))
                        .map(versionedAcls -> versionedAcls.acls)
                        .orElse(Collections.emptySet());

        Set<Resource> allowResources =
                RESOURCE_MAPPING
                        .getOrDefault(resource.getType(), r -> Collections.emptySet())
                        .apply(resource);

        Set<AccessControlEntry> literal = new HashSet<>();
        for (Resource allowResource : allowResources) {
            Optional.ofNullable(aclCacheSnapshot.get(allowResource))
                    .map(versionedAcls -> versionedAcls.acls)
                    .ifPresent(literal::addAll);
        }
        return Stream.of(wildcard, literal).flatMap(Set::stream).collect(Collectors.toSet());
    }

    @Override
    public void onFatalError(Throwable exception) {}

    private VersionedAcls getAclsFromCache(Resource resource) {
        if (aclCache.containsKey(resource)) {
            return aclCache.get(resource);
        }

        throw new IllegalArgumentException(
                String.format("ACLs do not exist in the cache for resource %s", resource));
    }

    private VersionedAcls getAclsFromZk(Resource resource) throws Exception {
        return zooKeeperClient.getResourceAclWithVersion(resource);
    }

    private static Set<FlussPrincipal> parseSuperUsers(Configuration configuration) {
        return configuration
                .getOptional(ConfigOptions.SUPER_USERS)
                .map(
                        config ->
                                Arrays.stream(config.split(";"))
                                        .map(String::trim)
                                        .map(
                                                user -> {
                                                    String[] userInfo = user.split(":");
                                                    return new FlussPrincipal(
                                                            userInfo[1], userInfo[0]);
                                                })
                                        .collect(Collectors.toSet()))
                .orElse(Collections.emptySet());
    }

    private void authorizeAclOperation(Session session, Collection<Resource> resources) {
        resources.forEach(
                resource -> {
                    // The minimum granularity of ACL operation permissions is Database.
                    authorize(
                            session,
                            OperationType.ALTER,
                            resource.getType() != ResourceType.TABLE
                                    ? resource
                                    : Resource.database(
                                            resource.getName().split(TABLE_SPLITTER)[0]));
                });
    }

    private int backoffTime(long backoffMs) {
        return (int) (backoffMs + (RETRY_BACKOFF_JITTER_MS * Math.random()));
    }

    /**
     * ZkNotificationHandler is responsible for processing ACL change notifications received from
     * ZooKeeper. It updates the internal cache based on the changes in ACLs for a specific
     * resource.
     */
    public class ZkNotificationHandler
            implements ZkNodeChangeNotificationWatcher.NotificationHandler {
        @Override
        public void processNotification(byte[] notification) throws Exception {
            synchronized (lock) {
                Resource resource = AclChangeNotificationNode.decode(notification);
                VersionedAcls versionedAcls = getAclsFromZk(resource);
                LOG.info(
                        "Processing Acl change notification for {}, acls : {}",
                        resource,
                        versionedAcls);
                updateCache(resource, versionedAcls);
            }
        }
    }

    // Orders by resource type, then resource pattern type and finally reverse ordering by name.
    private static class ResourceOrdering implements Comparator<Resource> {
        @Override
        public int compare(Resource a, Resource b) {
            int rt = a.getType().compareTo(b.getType());
            return rt != 0 ? rt : (-1) * a.getName().compareTo(b.getName());
        }
    }

    private static class ResourceTypeKey {
        private final AccessControlEntry accessControlEntry;
        private final ResourceType resourceType;

        public ResourceTypeKey(AccessControlEntry accessControlEntry, ResourceType resourceType) {
            this.accessControlEntry = accessControlEntry;
            this.resourceType = resourceType;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResourceTypeKey that = (ResourceTypeKey) o;
            return Objects.equals(accessControlEntry, that.accessControlEntry)
                    && resourceType == that.resourceType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(accessControlEntry, resourceType);
        }

        @Override
        public String toString() {
            return "ResourceTypeKey{"
                    + "accessControlEntry="
                    + accessControlEntry
                    + ", resourceType="
                    + resourceType
                    + '}';
        }
    }

    /**
     * VersionedAcls is a wrapper class that holds a set of AccessControlEntry objects along with
     * zknode version.
     */
    public static class VersionedAcls {
        public Set<AccessControlEntry> acls;
        int zkVersion;

        public VersionedAcls(int zkVersion, Set<AccessControlEntry> acls) {
            this.zkVersion = zkVersion;
            this.acls = acls;
        }

        boolean exists() {
            return zkVersion != UNKNOWN_VERSION;
        }

        @Override
        public String toString() {
            return "VersionedAcls{" + "acls=" + acls + ", zkVersion=" + zkVersion + '}';
        }
    }
}
