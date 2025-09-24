/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.fs.hdfs;

import org.apache.fluss.testutils.common.ClassLoaderUtils;
import org.apache.fluss.utils.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/** Tests that validate the behavior of the Hadoop File System Plugin. */
class HdfsFreeFsPluginTest {

    /**
     * This test validates that the plugin can be instantiated and configured even when Hadoop
     * classes are missing from the classpath.
     */
    @Test
    void testHadoopPluginInstantiationWithoutHadoop() throws Exception {
        // we do reflection magic here to instantiate the test in another class
        // loader, to make sure no hadoop classes are in the classpath

        final String testClassName = HdfsFreeTests.class.getCanonicalName();

        final URL[] urls = ClassLoaderUtils.getClasspathURLs();

        ClassLoader parent = getClass().getClassLoader();
        ClassLoader hadoopFreeClassLoader = new HdfsFreeClassLoader(urls, parent);
        Class<?> testClass = Class.forName(testClassName, false, hadoopFreeClassLoader);
        Method m = testClass.getDeclaredMethod("test");

        try {
            m.invoke(null);
        } catch (InvocationTargetException e) {
            ExceptionUtils.rethrowException(e.getTargetException(), "exception in method");
        }
    }

    // ------------------------------------------------------------------------

    private static final class HdfsFreeClassLoader extends URLClassLoader {

        private final ClassLoader properParent;

        HdfsFreeClassLoader(URL[] urls, ClassLoader parent) {
            super(urls, null);
            properParent = parent;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.startsWith("org.apache.hadoop")) {
                throw new ClassNotFoundException(name);
            } else if (name.startsWith("org.apache.log4j")) {
                return properParent.loadClass(name);
            } else {
                return super.loadClass(name);
            }
        }
    }
}
