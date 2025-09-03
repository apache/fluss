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

package org.apache.fluss.server.zk;

import java.util.List;

/** ZooKeeper response for async get children paths. */
public class ZookeeperGetChildrenResponse {
    private String path;
    private boolean success;
    private List<String> children;

    public ZookeeperGetChildrenResponse(String path, boolean success) {
        this.path = path;
        this.success = success;
    }

    public ZookeeperGetChildrenResponse(String path, boolean success, List<String> children) {
        this.path = path;
        this.success = success;
        this.children = children;
    }

    public String getPath() {
        return path;
    }

    public boolean isSuccess() {
        return success;
    }

    public List<String> getChildren() {
        return children;
    }
}
