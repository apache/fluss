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

import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.rpc.netty.server.Session;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Abstract Authorizer class provides common functionality for authorizing operations on resources.
 */
public abstract class AbstractAuthorizer implements Authorizer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractAuthorizer.class);

    @Override
    public boolean isAuthorized(Session session, OperationType operationType, Resource resource) {
        return session.isInternal()
                || authorizeAction(session, new Action(resource, operationType));
    }

    @Override
    public void authorize(Session session, OperationType operationType, Resource resource)
            throws AuthorizationException {
        // internal request will not be authorized
        if (!isAuthorized(session, operationType, resource)) {
            LOG.warn(
                    "Principal {} have no authorization to operate {} on resource {}",
                    session.getPrincipal(),
                    operationType,
                    resource);
            throw new AuthorizationException(
                    String.format(
                            "Principal %s have no authorization to operate %s on resource %s ",
                            session.getPrincipal(), operationType, resource));
        }
    }

    @Override
    public Collection<Resource> filterByAuthorized(
            Session session, OperationType operation, List<Resource> resources) {

        List<Action> actions =
                resources.stream()
                        .map(resource -> new Action(resource, operation))
                        .collect(Collectors.toList());

        List<Boolean> results = authorizeActions(session, actions);

        return IntStream.range(0, results.size())
                .filter(results::get)
                .mapToObj(actions::get)
                .map(Action::getResource)
                .collect(Collectors.toSet());
    }

    /**
     * Authorizes multiple actions for a given session.
     *
     * @param session the session associated with the request
     * @param action a action to be authorized
     * @return boolean results a boolean indicating whether the action is authorized
     */
    abstract boolean authorizeAction(Session session, Action action);

    public List<Boolean> authorizeActions(Session session, List<Action> actions) {
        return actions.stream()
                .map(action -> authorizeAction(session, action))
                .collect(Collectors.toList());
    }
}
