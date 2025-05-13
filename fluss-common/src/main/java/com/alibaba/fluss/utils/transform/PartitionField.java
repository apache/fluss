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

package com.alibaba.fluss.utils.transform;

import com.alibaba.fluss.metadata.Schema;

import java.io.Serializable;
import java.util.Objects;

/** A field in PartitionSpec which encodes the transform used on the column. */
public class PartitionField implements Serializable {
  private final Schema.Column sourceColumn;
  private final String targetName;
  private final Transform<?, ?> transform;

  public PartitionField(Schema.Column sourceColumn, String targetName, Transform<?, ?> transform) {
    this.sourceColumn = sourceColumn;
    this.targetName = targetName;
    this.transform = transform;
  }

  public Schema.Column getSourceColumn() {
    return sourceColumn;
  }

  public String getTargetName() {
    return targetName;
  }

  public Transform transform() {
    return transform;
  }

  public String toString() {
    return targetName + ": " + transform + "(" + sourceColumn.getName() + ")";
  }

  public int hashCode() {
    return Objects.hash(sourceColumn, targetName, transform);
  }
}
