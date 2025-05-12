/*
 *  Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.utils.transform;

import com.alibaba.fluss.metadata.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * NewPartitionSpec holds the description of how a partition key will be determined from source
 * columns. This can be one existing column, or it can be a compound partition key. NewPartitionSpec
 * encodes the transforms that the columns will use to make a partition key. Each column takes a
 * transform which must be compatible with the column's type. If not transform is desired, the
 * identity transform is used.
 */
public class NewPartitionSpec implements Serializable {
  private final Schema schema;
  private final List<PartitionField> fields;

  private NewPartitionSpec(Schema schema, List<PartitionField> fields) {
    this.schema = schema;
    this.fields = fields;
  }

  public String toString() {
    return "PartitionSpec{" + schema + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public int hashCode() {
    return Objects.hash(schema, fields);
  }

  /** Builder for newPartitionSpec. */
  public static final class Builder {
    private Schema schema;
    private final List<PartitionField> fields;

    private Builder() {
      fields = new ArrayList<>();
      schema = null;
    }

    public Builder withSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    private Schema.Column findSourceColumn(String sourceName) {
      int index = schema.getColumnIndexes(Collections.singletonList(sourceName))[0];
      return schema.getColumns().get(index);
    }

    public Builder year(String sourceName, String targetName) {
      year(findSourceColumn(sourceName), targetName);
      return this;
    }

    private Builder year(Schema.Column sourceColumn, String targetName) {
      PartitionField field = new PartitionField(sourceColumn, targetName, Transforms.years());
      fields.add(field);
      return this;
    }

    public Builder day(String sourceName, String targetName) {
      day(findSourceColumn(sourceName), targetName);
      return this;
    }

    private Builder day(Schema.Column sourceColumn, String targetName) {
      PartitionField field = new PartitionField(sourceColumn, targetName, Transforms.days());
      fields.add(field);
      return this;
    }

    public Builder month(String sourceName, String targetName) {
      month(findSourceColumn(sourceName), targetName);
      return this;
    }

    private Builder month(Schema.Column sourceColumn, String targetName) {
      PartitionField field = new PartitionField(sourceColumn, targetName, Transforms.months());
      fields.add(field);
      return this;
    }

    public Builder hour(String sourceName, String targetName) {
      hour(findSourceColumn(sourceName), targetName);
      return this;
    }

    private Builder hour(Schema.Column sourceColumn, String targetName) {
      PartitionField field = new PartitionField(sourceColumn, targetName, Transforms.hours());
      fields.add(field);
      return this;
    }

    public Builder identity(String sourceName, String targetName) {
      identity(findSourceColumn(sourceName), targetName);
      return this;
    }

    private Builder identity(Schema.Column sourceColumn, String targetName) {
      PartitionField field = new PartitionField(sourceColumn, targetName, Transforms.identity());
      fields.add(field);
      return this;
    }

    public NewPartitionSpec build() {
      return new NewPartitionSpec(schema, fields);
    }
  }
}
