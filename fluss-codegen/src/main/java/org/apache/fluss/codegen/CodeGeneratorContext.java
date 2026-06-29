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

package org.apache.fluss.codegen;

import org.apache.fluss.utils.InstantiationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The context for code generator, maintaining various reusable statements that could be inserted
 * into different code sections in the final generated class.
 */
public class CodeGeneratorContext {

    private static final AtomicLong NAME_COUNTER = new AtomicLong(0);

    /** Holding a list of objects that could be passed into generated class. */
    private final List<Object> references = new ArrayList<>();

    /** Set of member statements that will be added only once. */
    private final LinkedHashSet<String> reusableMemberStatements = new LinkedHashSet<>();

    /** Set of constructor statements that will be added only once. */
    private final LinkedHashSet<String> reusableInitStatements = new LinkedHashSet<>();

    public CodeGeneratorContext() {}

    /**
     * Adds a reusable member field statement to the member area.
     *
     * @param memberStatement the member field declare statement
     */
    public void addReusableMember(String memberStatement) {
        reusableMemberStatements.add(memberStatement);
    }

    /**
     * Adds a reusable Object to the member area of the generated class. The object must be
     * Serializable.
     *
     * @param obj the object to be added to the generated class (must be Serializable)
     * @param fieldNamePrefix prefix field name of the generated member field term
     * @param fieldTypeTerm field type class name
     * @return the generated unique field term
     */
    public <T extends Serializable> String addReusableObject(
            T obj, String fieldNamePrefix, String fieldTypeTerm) {
        String fieldTerm = newName(fieldNamePrefix);
        addReusableObjectInternal(obj, fieldTerm, fieldTypeTerm);
        return fieldTerm;
    }

    private <T extends Serializable> void addReusableObjectInternal(
            T obj, String fieldTerm, String fieldTypeTerm) {
        int idx = references.size();
        // make a deep copy of the object
        try {
            Object objCopy = InstantiationUtils.clone(obj);
            references.add(objCopy);
        } catch (Exception e) {
            throw new CodeGenException("Failed to clone object: " + obj, e);
        }

        reusableMemberStatements.add("private transient " + fieldTypeTerm + " " + fieldTerm + ";");
        reusableInitStatements.add(
                fieldTerm + " = ((" + fieldTypeTerm + ") references[" + idx + "]);");
    }

    /** Adds a reusable init statement which will be placed in constructor. */
    public void addReusableInitStatement(String statement) {
        reusableInitStatements.add(statement);
    }

    /**
     * @return code block of statements that need to be placed in the member area of the class
     */
    public String reuseMemberCode() {
        return String.join("\n", reusableMemberStatements);
    }

    /**
     * @return code block of statements that need to be placed in the constructor
     */
    public String reuseInitCode() {
        return String.join("\n", reusableInitStatements);
    }

    /**
     * @return the list of reference objects
     */
    public Object[] getReferences() {
        return references.toArray();
    }

    /**
     * Generates a new unique name with the given prefix.
     *
     * @param name the name prefix
     * @return a unique name
     */
    public static String newName(String name) {
        return name + "$" + NAME_COUNTER.getAndIncrement();
    }
}
