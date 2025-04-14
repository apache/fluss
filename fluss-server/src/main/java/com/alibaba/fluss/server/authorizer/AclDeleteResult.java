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

import com.alibaba.fluss.exception.ApiException;
import com.alibaba.fluss.security.acl.AclBinding;

import java.util.Collection;
import java.util.Optional;

/**
 * AclDeleteResult represents the result of an ACL (Access Control List) deletion operation. It
 * encapsulates the results of matching ACL filters and any exceptions encountered during the
 * process.
 */
public class AclDeleteResult {

    /** The exception encountered while attempting to match ACL filters for deletion, if any. */
    private final ApiException exception;

    /** The collection of delete results for each matching ACL binding. */
    private final Collection<AclBindingDeleteResult> aclBindingDeleteResults;

    public AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults) {
        this(deleteResults, null);
    }

    public AclDeleteResult(
            Collection<AclBindingDeleteResult> deleteResults, ApiException exception) {
        this.aclBindingDeleteResults = deleteResults;
        this.exception = exception;
    }

    public Optional<ApiException> exception() {
        return exception == null ? Optional.empty() : Optional.of(exception);
    }

    public Collection<AclBindingDeleteResult> aclBindingDeleteResults() {
        return aclBindingDeleteResults;
    }

    /**
     * AclBindingDeleteResult represents the result of deleting a specific ACL binding that matched
     * a delete filter.
     */
    public static class AclBindingDeleteResult {

        /** The ACL binding that matched the delete filter. */
        private final AclBinding aclBinding;

        /** The exception encountered while attempting to delete the ACL binding, if any. */
        private final ApiException exception;

        public AclBindingDeleteResult(AclBinding aclBinding, ApiException exception) {
            this.aclBinding = aclBinding;
            this.exception = exception;
        }

        public AclBinding aclBinding() {
            return aclBinding;
        }

        public Optional<ApiException> exception() {
            return exception == null ? Optional.empty() : Optional.of(exception);
        }
    }
}
