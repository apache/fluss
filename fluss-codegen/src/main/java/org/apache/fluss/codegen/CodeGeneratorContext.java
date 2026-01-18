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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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

    /**
     * The current method name for local variable statements. You can start a new local variable
     * statements for another method using {@link #startNewLocalVariableStatement(String)}.
     */
    private String currentMethodNameForLocalVariables = "DEFAULT";

    /** Map of method_name -> local_variable_statements. */
    private final Map<String, LinkedHashSet<String>> reusableLocalVariableStatements =
            new HashMap<>();

    public CodeGeneratorContext() {
        reusableLocalVariableStatements.put(
                currentMethodNameForLocalVariables, new LinkedHashSet<>());
    }

    /**
     * Starts a new local variable statements for a generated class with the given method name.
     *
     * @param methodName the method name which the fields will be placed into
     */
    public void startNewLocalVariableStatement(String methodName) {
        currentMethodNameForLocalVariables = methodName;
        reusableLocalVariableStatements.put(methodName, new LinkedHashSet<>());
    }

    /**
     * Adds a reusable local variable statement with the given type term and field name.
     *
     * @param fieldTypeTerm the field type term
     * @param fieldName the field name prefix
     * @return a new generated unique field name
     */
    public String addReusableLocalVariable(String fieldTypeTerm, String fieldName) {
        String fieldTerm = newName(fieldName);
        LinkedHashSet<String> statements =
                reusableLocalVariableStatements.computeIfAbsent(
                        currentMethodNameForLocalVariables, k -> new LinkedHashSet<>());
        statements.add(fieldTypeTerm + " " + fieldTerm + ";");
        return fieldTerm;
    }

    /**
     * Adds multiple pairs of local variables.
     *
     * @param fieldTypeAndNames pairs of (fieldTypeTerm, fieldName)
     * @return the new generated unique field names for each variable pair
     */
    public String[] addReusableLocalVariables(String[]... fieldTypeAndNames) {
        String[] fieldTerms = new String[fieldTypeAndNames.length];
        long newId = NAME_COUNTER.getAndIncrement();
        for (int i = 0; i < fieldTypeAndNames.length; i++) {
            String fieldTypeTerm = fieldTypeAndNames[i][0];
            String fieldName = fieldTypeAndNames[i][1];
            String fieldTerm = fieldName + "$" + newId;
            fieldTerms[i] = fieldTerm;
            LinkedHashSet<String> statements =
                    reusableLocalVariableStatements.computeIfAbsent(
                            currentMethodNameForLocalVariables, k -> new LinkedHashSet<>());
            statements.add(fieldTypeTerm + " " + fieldTerm + ";");
        }
        return fieldTerms;
    }

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
     * @param methodName the method name to get local variable code for
     * @return code block of local variable declarations for the specified method
     */
    public String reuseLocalVariableCode(String methodName) {
        LinkedHashSet<String> statements = reusableLocalVariableStatements.get(methodName);
        if (statements == null) {
            return "";
        }
        return String.join("\n", statements);
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
