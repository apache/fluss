<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Flink Multi-Version Tests

The Flink connector shares most test implementations from `fluss-flink-common` and runs them
through version-specific test classes in `fluss-flink-${flink.version}` modules. To keep CI time
under control, Flink 1.20 is the default test version and runs the complete test suite. Other Flink
versions run only tests marked with `@MultiVersionTest`.

This mechanism applies to every JUnit test in a filtered version module, including both `*ITCase`
classes and regular unit test classes.

## Execution Model

| Flink module       | Tests that run |
|--------------------| --- |
| `fluss-flink-1.20` | All tests |
| `fluss-flink-1.18` | Only tests marked with `@MultiVersionTest` |
| `fluss-flink-1.19` | Only tests marked with `@MultiVersionTest` |
| `fluss-flink-2.2`  | Only tests marked with `@MultiVersionTest` |
| `fluss-flink-2.3`  | Only tests marked with `@MultiVersionTest` |

The non-default modules enable JUnit extension auto-detection and multi-version filtering in their
`src/test/resources/junit-platform.properties` files:

```properties
junit.jupiter.extensions.autodetection.enabled=true
fluss.tests.multiversion.enabled=true
```

[`MultiVersionTestCondition`](../../fluss-test-utils/src/main/java/org/apache/fluss/testutils/common/MultiVersionTestCondition.java)
disables an unmarked test before its test lifecycle starts. A class with no selected methods is
disabled as a whole. Flink 1.20 does not contain this configuration, so the condition leaves all
of its tests enabled.

## Marking a Multi-Version Test

Add `@MultiVersionTest` to the smallest test scope that exercises a Flink compatibility boundary.
For example, annotate one representative method when the other methods in the class only verify
version-independent Fluss behavior:

```java
import org.apache.fluss.testutils.common.MultiVersionTest;

import org.junit.jupiter.api.Test;

class FlinkCatalogITCase {

    @Test
    @MultiVersionTest
    void testCreateTable() throws Exception {
        // Test a representative catalog operation against every supported Flink version.
    }

    @Test
    void testVersionIndependentBehavior() throws Exception {
        // Runs only with Flink 1.20.
    }
}
```

The annotation is inherited from shared test methods, so the version-specific subclasses do not
need to repeat it.

Annotate the class only when every test method in the class must run against every supported Flink
version:

```java
@MultiVersionTest
class FlinkAdapterTest {

    @Test
    void testFirstCompatibilityPath() {}

    @Test
    void testSecondCompatibilityPath() {}
}
```

Class-level annotations are also inherited by version-specific subclasses. Use them sparingly,
because every current and future test method added to the class becomes a multi-version test.

Good candidates for multi-version coverage include:

- Flink APIs whose signatures or behavior differ between supported versions.
- Planner, catalog, connector, serialization, or runtime integration points that have caused a
  version-specific regression.
- A small representative end-to-end path that verifies an important compatibility contract.

Do not mark every method just because its test class has version-specific subclasses. Functional
variants that exercise the same compatibility path should normally run only with Flink 1.20.

## Adding a Shared Test

Shared integration tests normally live in `fluss-flink-common/src/test/java`. Version modules use
small subclasses to bind the shared test to their Flink dependencies:

```java
// fluss-flink-common/src/test/java/.../FlinkExampleITCase.java
abstract class FlinkExampleITCase {

    @Test
    void testDefaultBehavior() {
        // Runs only with Flink 1.20 because it is not marked.
    }

    @Test
    @MultiVersionTest
    void testCompatibilityBoundary() {
        // Runs with every supported Flink version.
    }
}
```

```java
// fluss-flink-1.20/src/test/java/.../Flink120ExampleITCase.java
public class Flink120ExampleITCase extends FlinkExampleITCase {}
```

Create equivalent thin subclasses in each supported version module when the shared test compiles
and is relevant there. In filtered modules, an unmarked inherited method is discovered but
disabled. A marked inherited method is selected automatically.

When adding a new method to an existing shared class:

1. Add the test to the shared class in `fluss-flink-common`.
2. Decide whether it covers a distinct Flink compatibility boundary.
3. Leave it unmarked for default-version-only coverage, or add method-level `@MultiVersionTest`
   for all-version coverage.
4. Prefer one representative multi-version method over multiple equivalent variants.
5. Run the complete Flink 1.20 suite and the filtered suites before submitting the change.

## Adding a Version-Specific Test

Put behavior that exists only in one Flink version in that version's test subclass or test class.
Tests in a non-default module still need `@MultiVersionTest`; otherwise the module-level condition
will disable them:

```java
public class Flink118ProcedureITCase extends FlinkProcedureITCase {

    @Test
    @MultiVersionTest
    void testFlink118SpecificSignature() {
        // The method exists only in this module, so it runs only with Flink 1.18.
    }
}
```

Although the annotation says “multi-version”, in this case it means that the method participates
in the compatibility-test selection. It cannot run in another version module because the method
is declared only in the Flink 1.18 module.

## Running the Tests

Run the complete default-version suite:

```bash
./mvnw verify -pl fluss-flink/fluss-flink-1.20 -am
```

Run all filtered compatibility suites:

```bash
./mvnw verify \
  -pl fluss-flink/fluss-flink-1.18,fluss-flink/fluss-flink-1.19,fluss-flink/fluss-flink-2.2,fluss-flink/fluss-flink-2.3 \
  -am
```

### IntelliJ IDEA

Run the version-specific subclass from the corresponding version module. For example, run
`Flink118CatalogITCase` to reproduce the Flink 1.18 selection or `Flink120CatalogITCase` to run the
complete catalog test class. IntelliJ loads the module's `junit-platform.properties` from its test
classpath, so no additional VM options are required.

Avoid running the shared base class directly when checking version-specific behavior: that uses the
`fluss-flink-common` classpath instead of the target version module and does not reproduce the CI
selection.

## Adding a New Flink Version

When introducing another `fluss-flink-${flink.version}` module:

1. Add thin version-specific subclasses for the shared tests that the new version supports.
2. If it is not the default version, add `src/test/resources/junit-platform.properties` with the
   two filtering properties shown above.
3. Do not add the filtering properties to the default version module; it must run the complete
   suite.
4. Run the default suite and the new module to confirm that unmarked tests run only in the default
   module and marked tests run in both.
5. If the default version changes, move the full-suite responsibility by removing the properties
   from the new default module and adding them to the previous default module.

## Review Checklist

- Does this test exercise a Flink-version compatibility boundary rather than only Fluss behavior?
- Is method-level `@MultiVersionTest` sufficient, or does every method really need class-level
  selection?
- Does each required version module have a subclass that discovers the shared test?
- Are version-specific tests in filtered modules explicitly marked?
- Does Flink 1.20 still run the complete suite?
- Do IntelliJ and Maven both run the version-specific subclass with the expected selection?
