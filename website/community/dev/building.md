---
sidebar_position: 1
title: Building Fluss
---

# Building Fluss from Source

This page covers how to build Fluss from sources.

In order to build Fluss you need to get the source code by [cloning the git repository](https://github.com/apache/fluss).

In addition, you need **Maven 3.8.6** and a **JDK** (Java Development Kit). Fluss requires **Java 11** to build.

To clone from git, enter:

```bash
git clone git@github.com:apache/fluss.git
```

If you want to build a specific release or release candidate, have a look at the existing tags using

```bash
git tag -n
```

and checkout the corresponding branch using

```bash
git checkout <tag>
```

The simplest way of building Fluss is by running:

```bash
mvn clean install -DskipTests
```

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Fluss binary (`install`).

:::tip
Using the included [Maven Wrapper](https://maven.apache.org/wrapper/) by replacing `mvn` with `./mvnw` ensures that the correct Maven version is used.
:::

To speed up the build you can:
- skip tests by using ` -DskipTests`
- use Maven's parallel build feature, e.g., `mvn package -T 1C` will attempt to build 1 module for each CPU core in parallel.

The build script will be:
```bash
mvn clean install -DskipTests -T 1C
```

**NOTE**:
- For local testing, it's recommend to use directory `${project}/build-target` in project.
- For deploying distributed cluster, it's recommend to use binary file named `fluss-xxx-bin.tgz`, the file is in directory `${project}/fluss-dist/target`.

## Shaded dependencies

Fluss relocates the third-party libraries it bundles into the
`org.apache.fluss.shaded.*` namespace, and the filesystem plugins use a
per-plugin namespace such as `org.apache.fluss.fs.shaded.oss.*`. An uber-jar
that ships a library at its original package can shadow the copy belonging to
the application or engine that loads it, which surfaces as `NoSuchMethodError`
or `NoClassDefFoundError` at runtime rather than as a build failure.

### Writing a relocation

**Name the packages the jar actually bundles, not their common prefix.** The
shade plugin rewrites *every* reference matching a `<relocation>` pattern,
including references to classes the jar does not contain. Relocating
`org.apache.commons` in a module that bundles only `commons-lang3` but
*references* `commons-cli` rewrites the commons-cli references too, pointing
them at a coordinate nothing can ever provide. That turns a soft dependency,
resolvable from the surrounding classpath, into a guaranteed failure:

```
java.lang.NoClassDefFoundError: org/apache/fluss/shaded/org/apache/commons/cli/ParseException
    at org.apache.hadoop.hdfs.server.namenode.NameNode.createNameNode(NameNode.java:1713)
```

**Leave packages bound to native code alone.** A JNI library exports symbols
that embed the Java package name, so renaming the package breaks the binding:

```
$ nm -gU libarrow_cdata_jni.dylib | grep Java_org_apache_arrow
Java_org_apache_arrow_c_jni_JniWrapper_exportArray
```

For this reason `org.apache.arrow` is not relocated in `fluss-lake-lance`, and
`io.netty.internal.tcnative` is excluded from the netty relocation in the
filesystem plugins. `org.apache.hadoop` is likewise never relocated anywhere in
the repository, because the Hadoop `FileSystem` SPI resolves implementations by
class name from configuration.

**Run `mvn clean`.** An incremental build reuses already-relocated classes in
`target/classes`, so a changed relocation pattern appears to have no effect
until the module is cleaned.

### Checking a build for leaks

`tools/ci/check_shaded_jars.py` scans built jars for classes sitting at a
forbidden package path. It needs only Python 3 and the standard library.

```bash
# one jar
python3 tools/ci/check_shaded_jars.py fluss-client/target/fluss-client-*.jar

# every uber-jar in the repo; quote the globs, the script expands them and
# skips original-/tests/sources/javadoc jars and distribution copies itself
python3 tools/ci/check_shaded_jars.py \
    'fluss-*/target/*.jar' 'fluss-*/*/target/*.jar' 'tools/ci/*/target/*.jar'
```

Each jar gets a table of per-package counts:

```
  package                  leaked      mrj  relocated   annot
  com/fasterxml                 0        0       2066       0
  io/netty                   1698        0          0       0  LEAK
  org/apache/hadoop          5912        -          -       -  (tracked, never fatal)
```

`leaked` counts classes at the original package path and `mrj` counts them under
`META-INF/versions/`, which the shade plugin does not relocate and which
therefore need a `<filter>` exclusion rather than a rename. `relocated` should
be non-zero wherever a package is bundled: zero in both `leaked` and
`relocated` means a `<relocations>` block was silently discarded and the classes
were dropped instead of renamed. `annot` counts annotation-only packages, which
are reported but never fail the check since the JVM ignores an annotation class
it cannot resolve.

Exit code is 0 when clean and 1 when a leak is found. Three other modes help
when changing shade configuration:

```bash
# every unshaded third-party package, not just the ones on the rule list --
# use this to discover leaks the rules do not yet cover
python3 tools/ci/check_shaded_jars.py --audit --audit-min 50 path/to/uber.jar

# relocated references the jar does not contain, i.e. a pattern that rewrote
# links to classes that were never bundled
python3 tools/ci/check_shaded_jars.py --dangling path/to/uber.jar

# compare a build against one recorded earlier, so an expected-but-unshaded
# package such as org.apache.hadoop can be checked for drift rather than zero
python3 tools/ci/check_shaded_jars.py --baseline before.json path/to/*.jar
python3 tools/ci/check_shaded_jars.py --compare  before.json path/to/*.jar
```

`--compare` also fails when a leak disappears without a matching rise in
relocated classes, which catches classes being stripped rather than renamed.
`--json` writes the report to stdout for use in CI, with all human-readable
output on stderr.

## Building the Rust client (fluss-rust)

The Rust client, language bindings, and examples live under `fluss-rust/` and build with Cargo. You need **Rust** (the toolchain pinned in `fluss-rust/rust-toolchain.toml`, currently 1.85+). The code generated from the canonical `fluss-rpc/src/main/proto/FlussApi.proto` is checked in, so **protoc** is only needed when the proto changes — run `fluss-rust/crates/fluss/regen.sh` and commit the result.

```bash
cd fluss-rust
cargo build --workspace --all-targets    # build everything
cargo test --workspace                    # unit tests
```

Integration tests start a Fluss cluster via Docker:

```bash
RUST_TEST_THREADS=1 cargo test --features integration_tests --workspace
```

The Python and C++ bindings build on top of the Rust crate:

```bash
cd fluss-rust/bindings/python && uv sync --extra dev && uv run maturin develop   # Python
cd fluss-rust/bindings/cpp && cmake -B build && cmake --build build              # C++
```

Before pushing, run the same checks CI does:

```bash
cd fluss-rust
cargo fmt --all -- --check
cargo clippy --all-targets --workspace -- -D warnings
cargo deny check licenses
```