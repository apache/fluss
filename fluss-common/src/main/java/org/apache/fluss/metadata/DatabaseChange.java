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

package org.apache.fluss.metadata;

import javax.annotation.Nullable;

import java.util.Objects;

/** {@link DatabaseChange} represents the modification of the Fluss Database. */
public interface DatabaseChange {

    static SetOption set(String key, String value) {
        return new SetOption(key, value);
    }

    static ResetOption reset(String key) {
        return new ResetOption(key);
    }

    static UpdateComment updateComment(@Nullable String comment) {
        return new UpdateComment(comment);
    }

    /**
     * A database change to set the database option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER DATABASE &lt;database_name&gt; SET '&lt;key&gt;' = '&lt;value&gt;';
     * </pre>
     */
    class SetOption implements DatabaseChange {

        private final String key;
        private final String value;

        private SetOption(String key, String value) {
            this.key = key;
            this.value = value;
        }

        /** Returns the Option key to set. */
        public String getKey() {
            return key;
        }

        /** Returns the Option value to set. */
        public String getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof SetOption)) {
                return false;
            }
            SetOption setOption = (SetOption) o;
            return Objects.equals(key, setOption.key) && Objects.equals(value, setOption.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "SetOption{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
        }
    }

    /**
     * A database change to reset the database option.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER DATABASE &lt;database_name&gt; RESET '&lt;key&gt;'
     * </pre>
     */
    class ResetOption implements DatabaseChange {

        private final String key;

        public ResetOption(String key) {
            this.key = key;
        }

        /** Returns the Option key to reset. */
        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ResetOption)) {
                return false;
            }
            ResetOption that = (ResetOption) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

        @Override
        public String toString() {
            return "ResetOption{" + "key='" + key + '\'' + '}';
        }
    }

    /**
     * A database change to set the database comment.
     *
     * <p>It is equal to the following statement:
     *
     * <pre>
     *    ALTER DATABASE &lt;database_name&gt; SET COMMENT '&lt;comment&gt;';
     * </pre>
     */
    class UpdateComment implements DatabaseChange {

        private final @Nullable String comment;

        private UpdateComment(@Nullable String comment) {
            this.comment = comment;
        }

        /** Returns the comment to set. */
        @Nullable
        public String getComment() {
            return comment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof UpdateComment)) {
                return false;
            }
            UpdateComment that = (UpdateComment) o;
            return Objects.equals(comment, that.comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(comment);
        }

        @Override
        public String toString() {
            return "UpdateComment{" + "comment='" + comment + '\'' + '}';
        }
    }
}
