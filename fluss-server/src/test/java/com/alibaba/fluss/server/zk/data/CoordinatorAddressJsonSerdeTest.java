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

package com.alibaba.fluss.server.zk.data;

import com.alibaba.fluss.utils.json.JsonSerdeTestBase;

/** Test for {@link com.alibaba.fluss.server.zk.data.CoordinatorAddressJsonSerde}. */
public class CoordinatorAddressJsonSerdeTest extends JsonSerdeTestBase<CoordinatorAddress> {

    CoordinatorAddressJsonSerdeTest() {
        super(CoordinatorAddressJsonSerde.INSTANCE);
    }

    @Override
    protected CoordinatorAddress[] createObjects() {
        CoordinatorAddress coordinatorAddress = new CoordinatorAddress("1", "localhost", 1001);
        return new CoordinatorAddress[] {coordinatorAddress};
    }

    @Override
    protected String[] expectedJsons() {
        return new String[] {"{\"version\":1,\"id\":\"1\",\"host\":\"localhost\",\"port\":1001}"};
    }
}
