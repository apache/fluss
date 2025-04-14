/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.client.admin;

import com.alibaba.fluss.exception.ApiException;
import com.alibaba.fluss.exception.UnknownServerException;
import com.alibaba.fluss.rpc.messages.DeleteAclsMatchingAcl;
import com.alibaba.fluss.rpc.messages.DropAclsFilterResult;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.alibaba.fluss.rpc.util.CommonRpcMessageUtils.toAclBinding;

/** Represents the result of a delete ACLs operation. */
public class DeleteAclsResult {

    /** A class containing either the deleted ACL binding or an exception if the delete failed. */
    public static class FilterResult {
        private final AclBinding binding;
        private final ApiException exception;

        FilterResult(AclBinding binding, ApiException exception) {
            this.binding = binding;
            this.exception = exception;
        }

        /** Return the deleted ACL binding or null if there was an error. */
        public AclBinding binding() {
            return binding;
        }

        /** Return an exception if the ACL delete was not successful or null if it was. */
        public ApiException exception() {
            return exception;
        }
    }

    /** A class containing the results of the delete ACLs operation. */
    public static class FilterResults {
        private final List<FilterResult> values;

        FilterResults(List<FilterResult> values) {
            this.values = values;
        }

        /** Return a list of delete ACLs results for a given filter. */
        public List<FilterResult> values() {
            return values;
        }
    }

    private final Map<AclBindingFilter, CompletableFuture<FilterResults>> futures;

    DeleteAclsResult(Map<AclBindingFilter, CompletableFuture<FilterResults>> futures) {
        this.futures = futures;
    }

    /**
     * Return a future which succeeds only if all the ACLs deletions succeed, and which contains all
     * the deleted ACLs. Note that it if the filters don't match any ACLs, this is not considered an
     * error.
     */
    public CompletableFuture<Collection<AclBinding>> all() {
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .thenApply(v -> getAclBindings(futures));
    }

    private List<AclBinding> getAclBindings(
            Map<AclBindingFilter, CompletableFuture<FilterResults>> futures) {
        List<AclBinding> acls = new ArrayList<>();
        for (CompletableFuture<FilterResults> value : futures.values()) {
            FilterResults results;
            try {
                results = value.get();
            } catch (Throwable e) {
                // This should be unreachable, since the future returned by KafkaFuture#allOf should
                // have failed if any Future failed.
                throw new IllegalStateException("DeleteAclsResult#all: internal error", e);
            }
            for (FilterResult result : results.values()) {
                if (result.exception() != null) {
                    throw result.exception();
                }
                acls.add(result.binding());
            }
        }
        return acls;
    }

    public void completeExceptionally(Throwable t) {
        futures.values().forEach(future -> future.completeExceptionally(t));
    }

    public void complete(List<DropAclsFilterResult> results) {
        Iterator<DropAclsFilterResult> iter = results.iterator();
        for (AclBindingFilter bindingFilter : futures.keySet()) {
            CompletableFuture<FilterResults> future = futures.get(bindingFilter);
            if (!iter.hasNext()) {
                future.completeExceptionally(
                        new UnknownServerException(
                                "The broker reported no deletion result for the given filter."));
            } else {
                DropAclsFilterResult filterResult = iter.next();
                if (filterResult.hasErrorCode()
                        && filterResult.getErrorCode() != Errors.NONE.code()) {
                    ApiError apiError =
                            new ApiError(
                                    Errors.forCode(filterResult.getErrorCode()),
                                    filterResult.hasErrorMessage()
                                            ? filterResult.getErrorMessage()
                                            : null);
                    future.completeExceptionally(apiError.exception());
                } else {
                    List<FilterResult> filterResults = new ArrayList<>();
                    for (DeleteAclsMatchingAcl matchingAcl : filterResult.getMatchingAclsList()) {
                        if (matchingAcl.hasErrorCode()
                                && matchingAcl.getErrorCode() != Errors.NONE.code()) {
                            ApiError aclError =
                                    new ApiError(
                                            Errors.forCode(filterResult.getErrorCode()),
                                            filterResult.hasErrorMessage()
                                                    ? filterResult.getErrorMessage()
                                                    : null);
                            AclBinding aclBinding = toAclBinding(matchingAcl.getAcl());
                            filterResults.add(new FilterResult(aclBinding, aclError.exception()));
                        } else {
                            filterResults.add(
                                    new FilterResult(toAclBinding(matchingAcl.getAcl()), null));
                        }
                    }
                    future.complete(new FilterResults(filterResults));
                }
            }
        }
    }

    /**
     * Return a map from acl filters to futures which can be used to check the status of the
     * deletions by each filter.
     */
    private Map<AclBindingFilter, CompletableFuture<FilterResults>> values() {
        return futures;
    }
}
