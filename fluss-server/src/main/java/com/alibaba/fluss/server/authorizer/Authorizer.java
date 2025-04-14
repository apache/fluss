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

import com.alibaba.fluss.rpc.netty.server.Session;
import com.alibaba.fluss.security.acl.AclBinding;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** authorizer. */
public interface Authorizer extends Closeable {

    void startup() throws Exception;

    void close();

    default Boolean authorize(Session session, OperationType operationType, Resource resource) {
        return authorize(session, Collections.singletonList(new Action(resource, operationType)))
                .get(0);
    }

    List<Boolean> authorize(Session session, List<Action> actions);

    List<AclCreateResult> addAcls(List<AclBinding> aclBindings);

    List<AclDeleteResult> dropAcls(List<AclBindingFilter> filters);

    Collection<AclBinding> listAcls(AclBindingFilter filter);
}
