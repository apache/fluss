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

package com.alibaba.fluss.cluster;

import javax.annotation.Nullable;

import java.util.Objects;

/** Tablet server info. */
public class TabletServerInfo {
    private final int id;

    private @Nullable final String rack;

    public TabletServerInfo(int id, @Nullable String rack) {
        this.id = id;
        this.rack = rack;
    }

    public int getId() {
        return id;
    }

    public @Nullable String getRack() {
        return rack;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TabletServerInfo that = (TabletServerInfo) o;
        return id == that.id && Objects.equals(rack, that.rack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rack);
    }
}
