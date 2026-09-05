/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.kafka.schema;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Ordered physical Fluss fields populated by one Kafka record component. */
@Internal
public final class KafkaFieldProjection {

    private final List<Integer> positions;
    private final List<String> names;
    private final List<DataType> dataTypes;

    /** Creates a projection from physical row positions. */
    public KafkaFieldProjection(RowType rowType, List<Integer> positions) {
        checkNotNull(rowType);
        checkNotNull(positions);
        List<Integer> positionCopy = new ArrayList<>(positions.size());
        List<String> projectedNames = new ArrayList<>(positions.size());
        List<DataType> projectedTypes = new ArrayList<>(positions.size());
        for (Integer position : positions) {
            checkArgument(
                    position != null && position >= 0 && position < rowType.getFieldCount(),
                    "Invalid Kafka field projection position %s.",
                    position);
            positionCopy.add(position);
            projectedNames.add(rowType.getFieldNames().get(position));
            projectedTypes.add(rowType.getTypeAt(position));
        }
        this.positions = Collections.unmodifiableList(positionCopy);
        this.names = Collections.unmodifiableList(projectedNames);
        this.dataTypes = Collections.unmodifiableList(projectedTypes);
    }

    /** Returns the number of projected fields. */
    public int size() {
        return positions.size();
    }

    /** Returns whether this projection owns no fields. */
    public boolean isEmpty() {
        return positions.isEmpty();
    }

    /** Returns the physical row position at the projection position. */
    public int positionAt(int projectionPosition) {
        return positions.get(projectionPosition);
    }

    /** Returns the physical field name at the projection position. */
    public String nameAt(int projectionPosition) {
        return names.get(projectionPosition);
    }

    /** Returns the physical data type at the projection position. */
    public DataType dataTypeAt(int projectionPosition) {
        return dataTypes.get(projectionPosition);
    }

    /** Returns the projected physical positions. */
    public List<Integer> positions() {
        return positions;
    }
}
