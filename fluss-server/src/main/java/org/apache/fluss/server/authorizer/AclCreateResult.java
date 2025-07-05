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

import org.apache.fluss.exception.ApiException;
import org.apache.fluss.security.acl.AclBinding;

import java.util.Optional;

/**
 * AclCreateResult represents the result of an ACL (Access Control List) creation operation. It
 * encapsulates either a successful creation result containing the {@link AclBinding} or an
 * exception indicating a failure during the operation.
 */
public class AclCreateResult {

    /** The ACL binding created as part of the operation, if successful. */
    private final AclBinding aclBinding;

    /** The exception encountered during the ACL creation operation, if any. */
    private final ApiException exception;

    public AclCreateResult(AclBinding aclBinding, ApiException exception) {
        this.aclBinding = aclBinding;
        this.exception = exception;
    }

    public static AclCreateResult success(AclBinding aclBinding) {
        return new AclCreateResult(aclBinding, null);
    }

    public Optional<ApiException> exception() {
        return exception == null ? Optional.empty() : Optional.of(exception);
    }

    public AclBinding getAclBinding() {
        return aclBinding;
    }
}
