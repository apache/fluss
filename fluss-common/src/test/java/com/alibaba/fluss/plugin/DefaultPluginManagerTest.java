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

package com.alibaba.fluss.plugin;

import com.alibaba.fluss.plugin.jar.pluginb.TestServiceB;
import com.alibaba.fluss.shaded.guava32.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultPluginManager}. */
public class DefaultPluginManagerTest extends PluginTestBase {

    @TempDir private Path tmp;

    private Collection<PluginDescriptor> descriptors;

    @BeforeEach
    public void setup() throws Exception {
        /*
         * We setup a plugin directory hierarchy and utilize DirectoryBasedPluginFinder to create the
         * descriptors:
         *
         * <pre>
         * tmp/plugins-root/
         *          |-------------A/
         *          |             |-plugin-a.jar
         *          |
         *          |-------------B/
         *                        |-plugin-b.jar
         * </pre>
         */
        final File pluginRootFolder = tmp.toFile();
        final Path pluginRootFolderPath = pluginRootFolder.toPath();
        final File pluginAFolder = new File(pluginRootFolder, "A");
        final File pluginBFolder = new File(pluginRootFolder, "B");
        checkState(pluginAFolder.mkdirs());
        checkState(pluginBFolder.mkdirs());
        Files.copy(locateJarFile(PLUGIN_A).toPath(), Paths.get(pluginAFolder.toString(), PLUGIN_A));
        Files.copy(locateJarFile(PLUGIN_B).toPath(), Paths.get(pluginBFolder.toString(), PLUGIN_B));
        final PluginFinder descriptorsFactory =
                new DirectoryBasedPluginFinder(pluginRootFolderPath);
        descriptors = descriptorsFactory.findPlugins();
        checkState(descriptors.size() == 2);
    }

    @Test
    void testPluginLoading() {

        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
        assertThat(serviceImplList.size()).isEqualTo(2);

        // check that all impl have unique classloader
        final Set<ClassLoader> classLoaders = Collections.newSetFromMap(new IdentityHashMap<>(3));
        classLoaders.add(PARENT_CLASS_LOADER);
        for (TestSpi testSpi : serviceImplList) {
            assertThat(testSpi.testMethod()).isNotNull();
            assertThat(classLoaders.add(testSpi.getClass().getClassLoader())).isTrue();
        }

        final List<OtherTestSpi> otherServiceImplList =
                Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
        assertThat(otherServiceImplList.size()).isEqualTo(1);
        for (OtherTestSpi otherTestSpi : otherServiceImplList) {
            assertThat(otherTestSpi.otherTestMethod()).isNotNull();
            assertThat(classLoaders.add(otherTestSpi.getClass().getClassLoader())).isFalse();
        }
    }

    @Test
    void classLoaderMustBeTheSameInsideAPlugin() {
        String[] parentPatterns = {TestSpi.class.getName(), OtherTestSpi.class.getName()};
        final PluginManager pluginManager =
                new DefaultPluginManager(descriptors, PARENT_CLASS_LOADER, parentPatterns);
        final List<TestSpi> serviceImplList = Lists.newArrayList(pluginManager.load(TestSpi.class));
        assertThat(serviceImplList.size()).isEqualTo(2);

        final List<OtherTestSpi> otherServiceImplList =
                Lists.newArrayList(pluginManager.load(OtherTestSpi.class));
        assertThat(otherServiceImplList.size()).isEqualTo(1);

        // instanceof with multiple classloaders works only this way
        final List<TestSpi> serviceBImplList =
                serviceImplList.stream()
                        .filter(s -> s.getClass().getName().equals(TestServiceB.class.getName()))
                        .collect(Collectors.toList());
        assertThat(serviceBImplList.size()).isEqualTo(1);
        assertThat(serviceBImplList.get(0).getClass().getClassLoader())
                .isEqualTo(otherServiceImplList.get(0).getClass().getClassLoader());
    }
}
