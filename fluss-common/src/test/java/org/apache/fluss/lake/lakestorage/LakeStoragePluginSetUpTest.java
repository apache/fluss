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

package org.apache.fluss.lake.lakestorage;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.IllegalConfigurationException;
import org.apache.fluss.plugin.PluginManager;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LakeStoragePluginSetUp}. */
class LakeStoragePluginSetUpTest {
    private static final String DUPLICATE_FORMAT = "setup-test-duplicate";
    private static final String UNIQUE_FORMAT = "setup-test-unique";

    @Test
    void testDuplicatePluginsInPluginManager() {
        assertDuplicate(
                "manager-duplicate",
                manager(new ManagerPlugin(), new OtherManagerPlugin()),
                ManagerPlugin.class,
                OtherManagerPlugin.class);
    }

    @Test
    void testDuplicatePluginsInSpi() {
        assertDuplicate(
                DUPLICATE_FORMAT, null, DuplicateSpiPlugin.class, OtherDuplicateSpiPlugin.class);
    }

    @Test
    void testDuplicatePluginsAcrossSources() {
        assertDuplicate(
                UNIQUE_FORMAT,
                manager(new CrossSourcePlugin()),
                CrossSourcePlugin.class,
                UniqueSpiPlugin.class);
    }

    @Test
    void testSameImplementationAcrossSourcesIsDuplicate() {
        assertDuplicate(
                UNIQUE_FORMAT,
                manager(new UniqueSpiPlugin()),
                UniqueSpiPlugin.class,
                UniqueSpiPlugin.class);
    }

    @Test
    void testUniqueSpiPluginIgnoresOtherDuplicateFormats() {
        assertThat(LakeStoragePluginSetUp.fromDataLakeFormat(UNIQUE_FORMAT, null))
                .isInstanceOf(PluginLakeStorageWrapper.class);
    }

    @Test
    void testUniquePluginManagerPluginIgnoresOtherDuplicateFormats() {
        assertThat(
                        LakeStoragePluginSetUp.fromDataLakeFormat(
                                "manager-duplicate", manager(new ManagerPlugin())))
                .isInstanceOf(PluginLakeStorageWrapper.class);
    }

    @Test
    void testNoMatchingPlugin() {
        assertNoMatch(null);
        assertNoMatch(manager(new ManagerPlugin()));
    }

    @Test
    void testFailsBeforeReadingThirdMatch() {
        PluginManager pluginManager =
                new PluginManager() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <P> Iterator<P> load(Class<P> service) {
                        return (Iterator<P>)
                                new Iterator<LakeStoragePlugin>() {
                                    private int count;

                                    @Override
                                    public boolean hasNext() {
                                        if (count >= 2) {
                                            throw new AssertionError(
                                                    "Must fail on the second match");
                                        }
                                        return true;
                                    }

                                    @Override
                                    public LakeStoragePlugin next() {
                                        count++;
                                        return new ManagerPlugin();
                                    }
                                };
                    }
                };
        assertDuplicate(
                "manager-duplicate", pluginManager, ManagerPlugin.class, ManagerPlugin.class);
    }

    private static void assertNoMatch(PluginManager pluginManager) {
        assertThatThrownBy(
                        () ->
                                LakeStoragePluginSetUp.fromDataLakeFormat(
                                        "setup-test-missing", pluginManager))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "No LakeStoragePlugin can be found for datalake format: setup-test-missing");
    }

    private static void assertDuplicate(
            String format, PluginManager pluginManager, Class<?> first, Class<?> second) {
        assertThatThrownBy(() -> LakeStoragePluginSetUp.fromDataLakeFormat(format, pluginManager))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessage(
                        "Multiple LakeStoragePlugins for datalake format '"
                                + format
                                + "' found: "
                                + withLocation(first)
                                + ", "
                                + withLocation(second)
                                + ". Remove one of them from the plugins directory"
                                + " or the classpath.");
    }

    private static String withLocation(Class<?> pluginClass) {
        return pluginClass.getName()
                + " ("
                + pluginClass.getProtectionDomain().getCodeSource().getLocation()
                + ")";
    }

    private static PluginManager manager(LakeStoragePlugin... plugins) {
        return new PluginManager() {
            @Override
            @SuppressWarnings("unchecked")
            public <P> Iterator<P> load(Class<P> service) {
                return service == LakeStoragePlugin.class
                        ? (Iterator<P>) Arrays.asList(plugins).iterator()
                        : Collections.emptyIterator();
            }
        };
    }

    /** Test plugin available through SPI. */
    public static class DuplicateSpiPlugin implements LakeStoragePlugin {
        @Override
        public String identifier() {
            return DUPLICATE_FORMAT;
        }

        @Override
        public LakeStorage createLakeStorage(Configuration configuration) {
            throw new UnsupportedOperationException();
        }
    }

    /** Second SPI plugin with the same identifier. */
    public static class OtherDuplicateSpiPlugin extends DuplicateSpiPlugin {}

    /** SPI plugin with a unique identifier. */
    public static class UniqueSpiPlugin extends DuplicateSpiPlugin {
        @Override
        public String identifier() {
            return UNIQUE_FORMAT;
        }
    }

    /** PluginManager plugin sharing the SPI identifier. */
    private static class CrossSourcePlugin extends UniqueSpiPlugin {}

    /** Plugin only available through PluginManager. */
    private static class ManagerPlugin extends DuplicateSpiPlugin {
        @Override
        public String identifier() {
            return "manager-duplicate";
        }
    }

    /** Second PluginManager plugin with the same identifier. */
    private static class OtherManagerPlugin extends ManagerPlugin {}
}
