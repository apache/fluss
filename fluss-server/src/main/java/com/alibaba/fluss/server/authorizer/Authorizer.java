/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.authorizer;

import com.alibaba.fluss.exception.AuthenticationException;
import com.alibaba.fluss.exception.AuthorizationException;
import com.alibaba.fluss.rpc.netty.server.Session;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;

/**
 * The {@code Authorizer} interface defines the contract for managing and enforcing access control
 * in a system. It provides methods for authorization, adding or removing ACLs (Access Control
 * Lists), and listing existing ACL bindings.
 */
public interface Authorizer extends Closeable {

    /**
     * Initializes the authorizer. This method should be called before any other methods are used.
     *
     * @throws Exception if an error occurs during initialization
     */
    void startup() throws Exception;

    /** Closes the authorizer and releases any associated resources. */
    void close();

    /**
     * Checks if a given session is authorized to perform a specific operation on a resource. This
     * method is used for authorization checks and returns a boolean result indicating whether the
     * session has the required permissions. It does not throw an exception if the session is
     * unauthorized.
     *
     * @param session the session associated with the request
     * @param operationType the type of operation being checked
     * @param resource the resource on which the operation is being performed
     * @return true if the session is authorized, false otherwise
     */
    boolean isAuthorized(Session session, OperationType operationType, Resource resource);

    /**
     * Checks if a given session is authorized to perform a specific operation on a resource. Unlike
     * {@link #isAuthorized(Session, OperationType, Resource)}, this method enforces authorization
     * by throwing an {@link AuthenticationException} if the session is not authorized to perform
     * the operation. It is intended for scenarios where access control must be strictly enforced.
     *
     * @param session the session associated with the request
     * @param operationType the type of operation being checked
     * @param resource the resource on which the operation is being performed
     * @throws AuthorizationException if the session is not authorized to perform the operation
     */
    void authorize(Session session, OperationType operationType, Resource resource)
            throws AuthorizationException;

    /**
     * Filters a collection of resource names based on the provided session, operation, resources.
     */
    Collection<Resource> filterByAuthorized(
            Session session, OperationType operation, List<Resource> resources);

    /**
     * Adds multiple ACL bindings to the system after verifying that the session has the required
     * 'alter' permission on the associated resources.
     *
     * @param session the session associated with the request
     * @param aclBindings a list of ACL bindings to add
     * @return a list of results indicating the outcome of each ACL creation attempt
     * @throws SecurityException if the session does not have the required 'alter' permission
     */
    List<AclCreateResult> addAcls(Session session, List<AclBinding> aclBindings);

    /**
     * Removes ACL bindings from the system based on the provided filters, after verifying that the
     * session has the required 'alter' permission on the associated resources.
     *
     * @param session the session associated with the request
     * @param filters a list of filters specifying which ACL bindings to remove
     * @return a list of results indicating the outcome of each ACL deletion attempt
     * @throws SecurityException if the session does not have the required 'alter' permission
     */
    List<AclDeleteResult> dropAcls(Session session, List<AclBindingFilter> filters);

    /**
     * Lists all ACL bindings that match the specified filter.
     *
     * @param filter the filter to apply when listing ACL bindings
     * @return a collection of matching ACL bindings
     */
    Collection<AclBinding> listAcls(Session session, AclBindingFilter filter);
}
