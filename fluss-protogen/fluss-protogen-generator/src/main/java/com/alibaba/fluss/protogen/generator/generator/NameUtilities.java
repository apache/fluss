/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.protogen.generator.generator;

/* This file is based on source code of org.jibx:jibx-bind, licensed under the BSD License. See
 * the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. */

/** Support methods for name conversions. */
public class NameUtilities {
    /**
     * Convert potentially plural name to singular form. TODO: internationalization?
     *
     * @param name base name
     * @return singularized name
     */
    public static String depluralize(String name) {
        if (name.endsWith("ies")) {
            return name.substring(0, name.length() - 3) + 'y';
        } else if (name.endsWith("sses")) {
            return name.substring(0, name.length() - 2);
        } else if (name.endsWith("s") && !name.endsWith("ss")) {
            return name.substring(0, name.length() - 1);
        } else if (name.endsWith("List")) {
            return name.substring(0, name.length() - 4);
        } else {
            return name;
        }
    }

    /**
     * Convert singular name to plural form. TODO: internationalization?
     *
     * @param name base name
     * @return plural name
     */
    public static String pluralize(String name) {

        // first check for already in plural form
        if (name.endsWith("List") || (name.endsWith("s") && !name.endsWith("ss"))) {
            return name;
        }

        // convert singular form to plural
        if (name.endsWith("y")
                && !name.endsWith("ay")
                && !name.endsWith("ey")
                && !name.endsWith("iy")
                && !name.endsWith("oy")
                && !name.endsWith("uy")) {
            if (name.equalsIgnoreCase("any")) {
                return name;
            } else {
                return name.substring(0, name.length() - 1) + "ies";
            }
        } else if (name.endsWith("ss")) {
            return name + "es";
        } else {
            return name + 's';
        }
    }
}
