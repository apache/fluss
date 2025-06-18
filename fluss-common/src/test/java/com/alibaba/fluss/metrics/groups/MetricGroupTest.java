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

package com.alibaba.fluss.metrics.groups;

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metrics.CharacterFilter;
import com.alibaba.fluss.metrics.MeterView;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.metrics.registry.MetricRegistryImpl;
import com.alibaba.fluss.metrics.registry.NOPMetricRegistry;
import com.alibaba.fluss.metrics.util.DummyCharacterFilter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link MetricGroup}. */
class MetricGroupTest {

    private MetricRegistry registry;

    private final MetricRegistryImpl exceptionOnRegister = new ExceptionOnRegisterRegistry();

    @BeforeEach
    void createRegistry() {
        this.registry = MetricRegistry.create(new Configuration(), null);
    }

    @AfterEach
    void shutdownRegistry() throws Exception {
        this.registry.closeAsync().get();
        this.registry = null;
    }

    @Test
    void sameGroupOnNameCollision() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "somegroup");

        String groupName = "sometestname";
        MetricGroup subgroup1 = group.addGroup(groupName);
        MetricGroup subgroup2 = group.addGroup(groupName);

        assertThat(subgroup1).isSameAs(subgroup2);
    }

    @Test
    void testAddVariable() {
        MetricRegistry registry = NOPMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(key);
        assertThat(variableValue).isEqualTo(value);

        String[] scopes = group.getScopeComponents();
        assertThat(scopes)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(scopes)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope = group.getLogicalScope(new DummyCharacterFilter(), '-');
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is present in logical scope.")
                .doesNotContain(value);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} on a {@link
     * GenericKeyMetricGroup} goes through the generic code path.
     */
    @Test
    void testDefinedVariableOnKeyGroup() {
        MetricRegistry registry = NOPMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key1 = "key1";
        String value1 = "value1";
        root.addGroup(key1, value1);

        String key2 = "key2";
        String value2 = "value2";
        MetricGroup group = root.addGroup(key1).addGroup(key2, value2);

        String variableValue = group.getAllVariables().get("value2");
        assertThat(variableValue).isNull();

        String[] scopes = group.getScopeComponents();
        assertThat(scopes)
                .withFailMessage("Key1 is missing from metric identifier.")
                .contains("key1");
        assertThat(scopes)
                .withFailMessage("Key2 is missing from metric identifier.")
                .contains("key2");
        assertThat(scopes)
                .withFailMessage("Value2 is missing from metric identifier.")
                .contains("value2");

        String logicalScope = group.getLogicalScope(new DummyCharacterFilter(), '-');
        assertThat(logicalScope)
                .withFailMessage("Key1 is missing from logical scope.")
                .contains(key1);
        assertThat(logicalScope)
                .withFailMessage("Key2 is missing from logical scope.")
                .contains(key2);
        assertThat(logicalScope)
                .withFailMessage("Value2 is missing from logical scope.")
                .contains(value2);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with
     * the key name already exists goes through the generic code path.
     */
    @Test
    void testNameCollisionForKeyAfterGenericGroup() {
        MetricRegistry registry = NOPMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key);
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(key);
        assertThat(variableValue).isNull();

        String[] scopes = group.getScopeComponents();
        assertThat(scopes)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(scopes)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope = group.getLogicalScope(new DummyCharacterFilter(), '-');
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is missing from logical scope.")
                .contains(value);
    }

    /**
     * Verifies that calling {@link MetricGroup#addGroup(String, String)} if a generic group with
     * the key and value name already exists goes through the generic code path.
     */
    @Test
    void testNameCollisionForKeyAndValueAfterGenericGroup() {
        MetricRegistry registry = NOPMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key).addGroup(value);
        MetricGroup group = root.addGroup(key, value);

        String variableValue = group.getAllVariables().get(key);
        assertThat(variableValue).isNull();

        String[] scopes = group.getScopeComponents();
        assertThat(scopes)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(scopes)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope = group.getLogicalScope(new DummyCharacterFilter(), '-');
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is missing from logical scope.")
                .contains(value);
    }

    /**
     * Verifies that existing key/value groups are returned when calling {@link
     * MetricGroup#addGroup(String)}.
     */
    @Test
    void testNameCollisionAfterKeyValueGroup() {
        MetricRegistry registry = NOPMetricRegistry.INSTANCE;
        GenericMetricGroup root =
                new GenericMetricGroup(registry, new DummyAbstractMetricGroup(registry), "root");

        String key = "key";
        String value = "value";

        root.addGroup(key, value);
        MetricGroup group = root.addGroup(key).addGroup(value);

        String variableValue = group.getAllVariables().get(key);
        assertThat(variableValue).isEqualTo(value);

        String[] scopes = group.getScopeComponents();
        assertThat(scopes)
                .withFailMessage("Key is missing from metric identifier.")
                .contains("key");
        assertThat(scopes)
                .withFailMessage("Value is missing from metric identifier.")
                .contains("value");

        String logicalScope = group.getLogicalScope(new DummyCharacterFilter(), '-');
        assertThat(logicalScope)
                .withFailMessage("Key is missing from logical scope.")
                .contains(key);
        assertThat(logicalScope)
                .withFailMessage("Value is present in logical scope.")
                .doesNotContain(value);
    }

    @Test
    void closedGroupDoesNotRegisterMetrics() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "testgroup");
        assertThat(group.isClosed()).isFalse();

        group.close();
        assertThat(group.isClosed()).isTrue();

        // these will fail is the registration is propagated
        group.counter("testcounter");
        group.gauge("testgauge", () -> null);
    }

    @Test
    void closedGroupCreatesClosedGroups() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "testgroup");
        assertThat(group.isClosed()).isFalse();

        group.close();
        assertThat(group.isClosed()).isTrue();

        AbstractMetricGroup subgroup = (AbstractMetricGroup) group.addGroup("test subgroup");
        assertThat(subgroup.isClosed()).isTrue();
    }

    @Test
    void addClosedGroupReturnsNewGroupInstance() {
        GenericMetricGroup mainGroup =
                new GenericMetricGroup(
                        exceptionOnRegister,
                        new DummyAbstractMetricGroup(exceptionOnRegister),
                        "mainGroup");

        AbstractMetricGroup subGroup = (AbstractMetricGroup) mainGroup.addGroup("subGroup");

        assertThat(subGroup.isClosed()).isFalse();

        subGroup.close();
        assertThat(subGroup.isClosed()).isTrue();

        AbstractMetricGroup newSubGroupWithSameNameAsClosedGroup =
                (AbstractMetricGroup) mainGroup.addGroup("subGroup");
        assertThat(newSubGroupWithSameNameAsClosedGroup.isClosed())
                .withFailMessage("The new subgroup should not be closed")
                .isFalse();
        assertThat(subGroup.isClosed())
                .withFailMessage("The old sub group is not modified")
                .isTrue();
    }

    @Test
    void tolerateMetricNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "testgroup");

        assertThat(group.counter(name)).isNotNull();
        assertThat(group.counter(name)).isNotNull();
    }

    @Test
    void tolerateMetricAndGroupNameCollisions() {
        final String name = "abctestname";
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "testgroup");

        assertThat(group.addGroup(name)).isNotNull();
        assertThat(group.counter(name)).isNotNull();
    }

    @Test
    void testMeter() {
        GenericMetricGroup group =
                new GenericMetricGroup(
                        registry, new DummyAbstractMetricGroup(registry), "testgroup");

        MeterView meterView = new MeterView(1);
        group.meter("testMeter", meterView);

        meterView.markEvent();
        meterView.update();
        assertThat(meterView.getRate()).isEqualTo(0.2);
    }

    // ------------------------------------------------------------------------

    private static class ExceptionOnRegisterRegistry extends MetricRegistryImpl {

        public ExceptionOnRegisterRegistry() {
            super(Collections.emptyList());
        }

        @Override
        public void register(Metric metric, String name, AbstractMetricGroup parent) {
            fail("Metric should never be registered");
        }

        @Override
        public void unregister(Metric metric, String name, AbstractMetricGroup parent) {
            fail("Metric should never be un-registered");
        }
    }

    // ------------------------------------------------------------------------

    /**
     * A dummy {@link AbstractMetricGroup} to be used when a group is required as an argument but
     * not actually used.
     */
    public static class DummyAbstractMetricGroup extends AbstractMetricGroup {

        public DummyAbstractMetricGroup(MetricRegistry registry) {
            super(registry, new String[0], null);
        }

        @Override
        protected void addMetric(String name, Metric metric) {}

        @Override
        protected String getGroupName(CharacterFilter filter) {
            return "foo";
        }

        @Override
        public MetricGroup addGroup(String name) {
            return new DummyAbstractMetricGroup(registry);
        }
    }
}
