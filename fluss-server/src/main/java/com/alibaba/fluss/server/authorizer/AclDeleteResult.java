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

import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.security.acl.AclBinding;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

/**
 * AclDeleteResult represents the result of an ACL (Access Control List) deletion operation. It
 * encapsulates the results of matching ACL filters and any exceptions encountered during the
 * process.
 */
public class AclDeleteResult {

    /** The error encountered while attempting to match ACL filters for deletion, if any. */
    @Nullable private final ApiError error;

    /** The collection of delete results for each matching ACL binding. */
    private final Collection<AclBindingDeleteResult> aclBindingDeleteResults;

    public AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults) {
        this(deleteResults, null);
    }

    public AclDeleteResult(
            Collection<AclBindingDeleteResult> deleteResults, @Nullable ApiError error) {
        this.aclBindingDeleteResults = deleteResults;
        this.error = error;
    }

    public Optional<ApiError> error() {
        return Optional.ofNullable(error);
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
        @Nullable private final ApiError error;

        public AclBindingDeleteResult(AclBinding aclBinding, ApiError apiError) {
            this.aclBinding = aclBinding;
            this.error = apiError;
        }

        public AclBinding aclBinding() {
            return aclBinding;
        }

        public Optional<ApiError> error() {
            return error == null ? Optional.empty() : Optional.of(error);
        }
    }
}
