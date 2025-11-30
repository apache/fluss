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

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.AccessControlEntry;
import org.apache.fluss.security.acl.AccessControlEntryFilter;
import org.apache.fluss.security.acl.AclBinding;
import org.apache.fluss.security.acl.AclBindingFilter;
import org.apache.fluss.security.acl.FlussPrincipal;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.PermissionType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.security.acl.ResourceFilter;
import org.apache.fluss.security.acl.ResourceType;
import org.apache.fluss.server.zk.NOPErrorHandler;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.ZooKeeperExtension;
import org.apache.fluss.server.zk.ZooKeeperUtils;
import org.apache.fluss.server.zk.data.ZkData;
import org.apache.fluss.testutils.common.AllCallbackWrapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.fluss.security.acl.OperationType.ALTER;
import static org.apache.fluss.security.acl.OperationType.CREATE;
import static org.apache.fluss.security.acl.OperationType.DESCRIBE;
import static org.apache.fluss.security.acl.OperationType.DROP;
import static org.apache.fluss.security.acl.OperationType.READ;
import static org.apache.fluss.security.acl.OperationType.WRITE;
import static org.apache.fluss.security.acl.ResourceType.DATABASE;
import static org.apache.fluss.testutils.common.CommonTestUtils.retry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link DefaultAuthorizer}. */
public class DefaultAuthorizerTest {
    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    private static final String ROOT_USER = "root";

    private DefaultAuthorizer authorizer;
    private DefaultAuthorizer authorizer2;
    private ZooKeeperClient zooKeeperClient;
    private Configuration configuration;

    @BeforeEach
    void setUp() throws Exception {
        configuration = new Configuration();
        configuration.setString(
                ConfigOptions.ZOOKEEPER_ADDRESS,
                ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().getConnectString());
        configuration.setString(ConfigOptions.SUPER_USERS, "USER:" + ROOT_USER);
        configuration.set(ConfigOptions.AUTHORIZER_ENABLED, true);
        zooKeeperClient = ZooKeeperUtils.startZookeeperClient(configuration, new NOPErrorHandler());
        authorizer =
                (DefaultAuthorizer)
                        AuthorizerLoader.createAuthorizer(configuration, zooKeeperClient, null);
        authorizer2 =
                (DefaultAuthorizer) AuthorizerLoader.createAuthorizer(configuration, null, null);
        authorizer.startup();
        authorizer2.startup();
    }

    @AfterEach
    void tearDown() throws Exception {
        authorizer.dropAcls(
                createRootUserSession(), Collections.singletonList(AclBindingFilter.ANY));
        authorizer.close();
        authorizer2.close();
        zooKeeperClient.close();
    }

    @Test
    void testSimpleAclOperation() throws Exception {
        assertThat(authorizer.listAcls(createRootUserSession(), AclBindingFilter.ANY)).isEmpty();
        List<AclBinding> database1Acls =
                Arrays.asList(
                        createAclBinding(Resource.database("database1"), "user1", "host-1", CREATE),
                        createAclBinding(Resource.database("database1"), "user2", "host-1", DROP));

        List<AclBinding> database2Acls =
                Arrays.asList(
                        createAclBinding(Resource.database("database2"), "user1", "host-1", CREATE),
                        createAclBinding(Resource.database("database2"), "user2", "host-1", DROP));

        authorizer.addAcls(createRootUserSession(), database1Acls);
        authorizer.addAcls(createRootUserSession(), database2Acls);
        // list by database
        assertThat(
                        authorizer.listAcls(
                                createRootUserSession(),
                                new AclBindingFilter(
                                        new ResourceFilter(DATABASE, "database2"),
                                        AccessControlEntryFilter.ANY)))
                .containsExactlyInAnyOrderElementsOf(database2Acls);
        // list by user
        assertThat(
                        authorizer.listAcls(
                                createRootUserSession(),
                                createAclBindingFilter(
                                        ResourceFilter.ANY,
                                        new FlussPrincipal("user1", "USER"),
                                        null,
                                        OperationType.ANY)))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(database1Acls.get(0), database2Acls.get(0)));
        // list by operation
        assertThat(
                        authorizer.listAcls(
                                createRootUserSession(),
                                createAclBindingFilter(
                                        ResourceFilter.ANY,
                                        FlussPrincipal.ANY,
                                        null,
                                        OperationType.CREATE)))
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.asList(database1Acls.get(0), database2Acls.get(0)));

        authorizer.dropAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(DATABASE, "database2"),
                                AccessControlEntryFilter.ANY)));
        assertThat(authorizer.listAcls(createRootUserSession(), AclBindingFilter.ANY))
                .containsExactlyInAnyOrderElementsOf(database1Acls);
    }

    @Test
    void testSimpleAuthorizations() throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        List<Action> actions =
                Arrays.asList(
                        new Action(Resource.table("database1", "foo"), READ),
                        new Action(Resource.database("database2"), WRITE));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(false, false);
        authorizer.addAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database2"),
                                new AccessControlEntry(
                                        session.getPrincipal(),
                                        "192.168.1.1",
                                        WRITE,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(false, true);
        authorizer.addAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBinding(
                                Resource.table("database1", "foo"),
                                new AccessControlEntry(
                                        session.getPrincipal(), "*", READ, PermissionType.ALLOW))));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(true, true);
        authorizer.dropAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBindingFilter(
                                new ResourceFilter(ResourceType.TABLE, "database1.foo"),
                                new AccessControlEntryFilter(
                                        null, null, READ, PermissionType.ANY))));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(false, true);
    }

    @Test
    void testAuthorizerNoZkConfig() {
        Configuration configuration =
                new Configuration().set(ConfigOptions.AUTHORIZER_ENABLED, true);
        assertThatThrownBy(() -> AuthorizerLoader.createAuthorizer(configuration, null, null))
                .hasMessageContaining(
                        "No valid ZooKeeper quorum has been specified. You can specify the quorum via the configuration key 'zookeeper.address");
    }

    @Test
    void testSuperUserHasAccess() throws Exception {
        Session normalUserSession = createSession("user1", "192.168.1.1");
        Session superUserSession = createSession("root", "192.168.1.1");
        assertThat(authorizer.isAuthorized(normalUserSession, READ, Resource.database("database1")))
                .isFalse();
        assertThat(authorizer.isAuthorized(superUserSession, READ, Resource.database("database1")))
                .isTrue();
    }

    @Test
    void testAclWithOperationAll() throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        List<Action> actions =
                Arrays.asList(
                        new Action(Resource.database("database1"), READ),
                        new Action(Resource.database("database1"), WRITE));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(false, false);
        authorizer.addAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        session.getPrincipal(),
                                        "192.168.1.1",
                                        OperationType.ALL,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorizeActions(session, actions)).containsExactly(true, true);
    }

    @Test
    void testHostAddressAclValidation() throws Exception {
        FlussPrincipal principal = new FlussPrincipal("user1", "USER");
        Session session1 = createSession("user1", "192.168.1.1");
        Session session2 = createSession("user1", "192.168.1.2");
        List<Action> actions =
                Collections.singletonList(new Action(Resource.database("database1"), READ));
        assertThat(authorizer.authorizeActions(session1, actions)).containsExactly(false);
        assertThat(authorizer.authorizeActions(session2, actions)).containsExactly(false);
        authorizer.addAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        principal,
                                        session1.getInetAddress().getHostAddress(),
                                        READ,
                                        PermissionType.ALLOW))));
        assertThat(authorizer.authorizeActions(session1, actions)).containsExactly(true);
        assertThat(authorizer.authorizeActions(session2, actions)).containsExactly(false);
        authorizer.addAcls(
                createRootUserSession(),
                Collections.singletonList(
                        new AclBinding(
                                Resource.database("database1"),
                                new AccessControlEntry(
                                        principal, "*", READ, PermissionType.ALLOW))));
        assertThat(authorizer.authorizeActions(session1, actions)).containsExactly(true);
        assertThat(authorizer.authorizeActions(session2, actions)).containsExactly(true);
    }

    /** Test ACL inheritance, as described in {@link OperationType}. */
    @Test
    void testAclInheritanceOnOperationType() throws Exception {
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(),
                OperationType.ALL,
                Arrays.asList(READ, WRITE, CREATE, DROP, ALTER, DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(), OperationType.CREATE, Collections.singleton(DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(), OperationType.DROP, Collections.singleton(DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(), ALTER, Collections.singleton(DESCRIBE));

        // when we allow READ on any resource, we also allow to DESCRIBE and FILESYSTEM_TOKEN on
        // cluster.
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(), READ, Collections.singletonList(DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.cluster(), WRITE, Collections.singletonList(DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.database("database1"), READ, Collections.singletonList(DESCRIBE));
        testOperationTypeImplicationsOfAllow(
                Resource.table("database2", "table1"), WRITE, Collections.singletonList(DESCRIBE));
    }

    private void testOperationTypeImplicationsOfAllow(
            Resource resource, OperationType parentOp, Collection<OperationType> allowedOps)
            throws Exception {
        Session session = createSession("user1", "192.168.1.1");
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(session.getPrincipal(), "*", parentOp, PermissionType.ALLOW);
        addAcls(authorizer, resource, Collections.singleton(accessControlEntry));
        Arrays.asList(OperationType.values())
                .forEach(
                        op -> {
                            if (allowedOps.contains(op) || op == parentOp) {
                                assertThat(authorizer.isAuthorized(session, op, resource)).isTrue();
                            } else if (op != OperationType.ANY) {
                                assertThat(authorizer.isAuthorized(session, op, resource))
                                        .isFalse();
                            }
                        });
        dropAcls(
                authorizer,
                resource,
                Collections.singleton(
                        createAclEntry(session.getPrincipal().getName(), "*", parentOp)));
    }

    @Test
    void testAclInheritanceOnResourceType() throws Exception {
        testResourceTypeImplicationsOfAllow(
                ResourceType.CLUSTER, Arrays.asList(DATABASE, ResourceType.TABLE));
        testResourceTypeImplicationsOfAllow(DATABASE, Collections.singleton(ResourceType.TABLE));
    }

    private void testResourceTypeImplicationsOfAllow(
            ResourceType parentType, Collection<ResourceType> allowedTypes) throws Exception {
        FlussPrincipal user = new FlussPrincipal("user1", "USER");
        Session session = createSession("user1", "192.168.1.1");
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(user, "*", READ, PermissionType.ALLOW);
        addAcls(authorizer, mockResource(parentType), Collections.singleton(accessControlEntry));
        Arrays.asList(ResourceType.values())
                .forEach(
                        resourceType -> {
                            if (allowedTypes.contains(resourceType) || resourceType == parentType) {
                                assertThat(
                                                authorizer.isAuthorized(
                                                        session, READ, mockResource(resourceType)))
                                        .isTrue();
                            } else if (resourceType != ResourceType.ANY) {
                                assertThat(
                                                authorizer.isAuthorized(
                                                        session, READ, mockResource(resourceType)))
                                        .isFalse();
                            }
                        });
        dropAcls(authorizer, mockResource(parentType), Collections.singleton(accessControlEntry));
    }

    private Resource mockResource(ResourceType resourceType) {
        switch (resourceType) {
            case CLUSTER:
                return Resource.cluster();
            case DATABASE:
                return Resource.database("database1");
            case TABLE:
                return Resource.table("database1", "table1");
            default:
                return null;
        }
    }

    @Test
    void testLoadCache() throws Exception {
        Resource resource1 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls1 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user1", "User"),
                                "host-1",
                                READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource1, acls1);

        Resource resource2 = Resource.database("foo-" + UUID.randomUUID());
        Set<AccessControlEntry> acls2 =
                Collections.singleton(
                        new AccessControlEntry(
                                new FlussPrincipal("user2", "User"),
                                "host-1",
                                READ,
                                PermissionType.ANY));
        addAcls(authorizer, resource2, acls2);

        // delete acl change notifications to test initial load.
        deleteAclChangeNotifications();

        try (Authorizer newAuthorizer =
                AuthorizerLoader.createAuthorizer(configuration, null, null)) {
            newAuthorizer.startup();
            assertThat(listAcls(newAuthorizer, resource1)).isEqualTo(acls1);
            assertThat(listAcls(newAuthorizer, resource2)).isEqualTo(acls2);

            // test update cache later
            final Set<AccessControlEntry> acls3 =
                    new HashSet<>(
                            Collections.singleton(
                                    new AccessControlEntry(
                                            new FlussPrincipal("user2", "User"),
                                            "host-1",
                                            READ,
                                            PermissionType.ANY)));
            addAcls(authorizer, resource2, acls3);
            retry(
                    Duration.ofMinutes(1),
                    () -> assertThat(listAcls(newAuthorizer, resource2)).isEqualTo(acls3));
        }
    }

    // Authorizing the empty resource is not supported because we create a znode with the resource
    // name.
    @Test
    void testEmptyAclThrowsException() {
        assertThatThrownBy(
                        () ->
                                addAcls(
                                        authorizer,
                                        Resource.database(""),
                                        Collections.singleton(
                                                new AccessControlEntry(
                                                        new FlussPrincipal("user1", "User"),
                                                        "host-1",
                                                        READ,
                                                        PermissionType.ANY))))
                .hasMessageContaining("Path must not end with / character");
    }

    @Test
    void testLocalConcurrentModificationOfResourceAcls() throws Exception {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 = new AccessControlEntry(user1, "host-1", READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 = new AccessControlEntry(user2, "host-2", READ, PermissionType.ANY);
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer, commonResource, Collections.singleton(acl2));
        assertThat(listAcls(authorizer, commonResource))
                .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
    }

    @Test
    void testDistributedConcurrentModificationOfResourceAcls() throws Exception {
        Resource commonResource = Resource.database("test");
        FlussPrincipal user1 = new FlussPrincipal("user1", "User");
        AccessControlEntry acl1 = new AccessControlEntry(user1, "host-1", READ, PermissionType.ANY);
        FlussPrincipal user2 = new FlussPrincipal("user2", "User");
        AccessControlEntry acl2 = new AccessControlEntry(user2, "host-2", READ, PermissionType.ANY);
        // Add on each instance
        addAcls(authorizer, commonResource, Collections.singleton(acl1));
        addAcls(authorizer2, commonResource, Collections.singleton(acl2));

        FlussPrincipal user3 = new FlussPrincipal("user3", "User");
        AccessControlEntry acl3 = new AccessControlEntry(user3, "host-3", READ, PermissionType.ANY);

        // Add on one instance and delete on another
        addAcls(authorizer, commonResource, Collections.singleton(acl3));
        dropAcls(authorizer2, commonResource, Collections.singleton(acl3));

        retry(
                Duration.ofMinutes(1),
                () ->
                        assertThat(listAcls(authorizer, commonResource))
                                .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2))));

        retry(
                Duration.ofMinutes(1),
                () -> {
                    assertThat(listAcls(authorizer2, commonResource))
                            .isEqualTo(new HashSet<>(Arrays.asList(acl1, acl2)));
                });
    }

    @Test
    void testHighConcurrencyModificationOfResourceAcls() throws Exception {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        // generate 50 concurrent acl operation tasks.
        List<Runnable> concurrentTasks = new ArrayList<>();
        Set<AccessControlEntry> expectedAcls = new HashSet<>();

        for (int i = 0; i < 50; i++) {
            // each task represent that add acl to different user on same resource.
            final AccessControlEntry accessControlEntry =
                    createAclEntry(String.valueOf(i), "host-1", READ);
            int finalI = i;
            Runnable runnable =
                    () -> {
                        if (finalI % 2 == 0) {
                            addAcls(
                                    authorizer,
                                    commonResource,
                                    Collections.singleton(accessControlEntry));
                        } else {
                            addAcls(
                                    authorizer2,
                                    commonResource,
                                    Collections.singleton(accessControlEntry));
                        }

                        if (finalI % 10 == 0) {
                            // If cache is still empty because modify notification is not arrived,
                            // AclBindingFilter will match nothing. Thus retry to make sure that the
                            // acl is deleted.
                            retry(
                                    Duration.ofMinutes(1),
                                    () ->
                                            assertThat(
                                                            dropAcls(
                                                                    authorizer2,
                                                                    commonResource,
                                                                    Collections.singleton(
                                                                            accessControlEntry)))
                                                    .containsExactly(
                                                            new AclBinding(
                                                                    commonResource,
                                                                    accessControlEntry)));
                        }
                    };
            concurrentTasks.add(runnable);

            if (finalI % 10 != 0) {
                expectedAcls.add(accessControlEntry);
            }
        }

        runInConcurrent(concurrentTasks);
        // Make sure all acl notifacations are arrived.
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(listAcls(authorizer, commonResource)).isEqualTo(expectedAcls));
        retry(
                Duration.ofMinutes(1),
                () -> assertThat(listAcls(authorizer2, commonResource)).isEqualTo(expectedAcls));
    }

    @Test
    void testHighConcurrencyDeletionOfResourceAcls() {
        Resource commonResource = Resource.database("foo-" + UUID.randomUUID());
        AccessControlEntry accessControlEntry =
                new AccessControlEntry(
                        new FlussPrincipal("user1", "User"), "host-1", READ, PermissionType.ANY);
        // generate 50 concurrent acl operation tasks.
        List<Runnable> concurrentTasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            // each task represent that add acl to same user on same resource.
            Runnable runnable =
                    () -> {
                        addAcls(
                                authorizer,
                                commonResource,
                                Collections.singleton(accessControlEntry));
                        dropAcls(
                                authorizer2,
                                commonResource,
                                Collections.singleton(accessControlEntry));
                    };
            concurrentTasks.add(runnable);
        }
        runInConcurrent(concurrentTasks);
        assertThat(listAcls(authorizer2, commonResource)).isEqualTo(Collections.emptySet());
    }

    void addAcls(Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBinding> aclBindings =
                entries.stream()
                        .map(entry -> new AclBinding(resource, entry))
                        .collect(Collectors.toList());
        authorizer
                .addAcls(createRootUserSession(), aclBindings)
                .forEach(
                        result -> {
                            if (result.exception().isPresent()) {
                                throw result.exception().get();
                            }
                        });
    }

    Set<AccessControlEntry> listAcls(Authorizer authorizer, Resource resource) {
        AclBindingFilter aclBindingFilter =
                new AclBindingFilter(
                        new ResourceFilter(resource.getType(), resource.getName()),
                        AccessControlEntryFilter.ANY);
        Collection<AclBinding> aclBindings =
                authorizer.listAcls(createRootUserSession(), aclBindingFilter);
        return aclBindings.stream()
                .map(AclBinding::getAccessControlEntry)
                .collect(Collectors.toSet());
    }

    List<AclBinding> dropAcls(
            Authorizer authorizer, Resource resource, Set<AccessControlEntry> entries) {
        List<AclBindingFilter> aclBindings =
                entries.stream()
                        .map(
                                entry ->
                                        new AclBindingFilter(
                                                new ResourceFilter(
                                                        resource.getType(), resource.getName()),
                                                new AccessControlEntryFilter(
                                                        entry.getPrincipal(),
                                                        entry.getHost(),
                                                        entry.getOperationType(),
                                                        entry.getPermissionType())))
                        .collect(Collectors.toList());
        List<AclBinding> deleteAclBindings = new ArrayList<>();
        authorizer
                .dropAcls(createRootUserSession(), aclBindings)
                .forEach(
                        result -> {
                            if (result.error().isPresent()) {
                                throw result.error().get().exception();
                            } else {
                                deleteAclBindings.addAll(
                                        result.aclBindingDeleteResults().stream()
                                                .map(
                                                        AclDeleteResult.AclBindingDeleteResult
                                                                ::aclBinding)
                                                .collect(Collectors.toList()));
                            }
                        });
        return deleteAclBindings;
    }

    /**
     * Asserts that a list of tasks can be executed concurrently within a given timeout.
     *
     * @param tasks the list of tasks to execute
     */
    private void runInConcurrent(List<Runnable> tasks) {
        ExecutorService executor = Executors.newFixedThreadPool(tasks.size());
        List<Future<?>> futures = tasks.stream().map(executor::submit).collect(Collectors.toList());

        executor.shutdown();
        try {
            boolean completed = executor.awaitTermination(30000, TimeUnit.MILLISECONDS);
            assertThat(completed).isTrue();
            for (Future<?> future : futures) {
                future.get(); // Ensure no exceptions were thrown
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Should support many concurrent calls"
                            + " - Exception during concurrent execution",
                    e);
        }
    }

    private Session createRootUserSession() {
        try {
            return createSession(ROOT_USER, "127.0.0.1");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Session createSession(String username, String host) throws Exception {
        return new Session(
                (byte) 1,
                "FLUSS",
                false,
                InetAddress.getByName(host),
                new FlussPrincipal(username, "USER"));
    }

    private AclBinding createAclBinding(
            Resource resource, String username, String host, OperationType operation) {
        return new AclBinding(resource, createAclEntry(username, host, operation));
    }

    private AccessControlEntry createAclEntry(
            String username, String host, OperationType operation) {
        return new AccessControlEntry(
                new FlussPrincipal(username, "USER"), host, operation, PermissionType.ALLOW);
    }

    private AclBindingFilter createAclBindingFilter(
            ResourceFilter resourceFilter,
            FlussPrincipal flussPrincipal,
            String host,
            OperationType operation) {
        return new AclBindingFilter(
                resourceFilter,
                new AccessControlEntryFilter(
                        flussPrincipal, host, operation, PermissionType.ALLOW));
    }

    public void deleteAclChangeNotifications() throws Exception {
        List<String> children = zooKeeperClient.getChildren(ZkData.AclChangesNode.path());
        for (String child : children) {
            zooKeeperClient.deletePath(ZkData.AclChangesNode.path() + "/" + child);
        }
    }
}
