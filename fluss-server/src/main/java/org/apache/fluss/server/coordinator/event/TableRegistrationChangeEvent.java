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

package org.apache.fluss.server.coordinator.event;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.data.TableRegistration;

import java.util.Objects;

/** An event for table registration change. */
public class TableRegistrationChangeEvent implements CoordinatorEvent {
    private final TablePath tablePath;
    private final TableRegistration newTableRegistration;

    public TableRegistrationChangeEvent(
            TablePath tablePath, TableRegistration newTableRegistration) {
        this.tablePath = tablePath;
        this.newTableRegistration = newTableRegistration;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public TableRegistration getNewTableRegistration() {
        return newTableRegistration;
    }

    @Override
    public String toString() {
        return "TablePropertiesChangeEvent{"
                + "tablePath="
                + tablePath
                + ", tableRegistration="
                + newTableRegistration
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableRegistrationChangeEvent that = (TableRegistrationChangeEvent) o;
        return Objects.equals(tablePath, that.tablePath)
                && Objects.equals(newTableRegistration, that.newTableRegistration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, newTableRegistration);
    }
}
