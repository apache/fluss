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

package org.apache.fluss.flink.action;

import java.util.Optional;

/** Main entrypoint for the Fluss Flink action jar. Delegates to {@link ActionLoader}. */
public class FlussActionEntrypoint {

    public static void main(String[] args) throws Exception {
        Optional<Action> action;
        try {
            action = ActionLoader.createAction(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        if (!action.isPresent()) {
            return;
        }
        action.get().build();
        action.get().run();
    }
}
