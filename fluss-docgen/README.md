# Fluss Documentation Generator

This module contains utilities to automatically generate documentation parts from the Fluss source code. This ensures that the documentation stays in sync with the actual implementation and default values.

## Configuration Options Generator

The `ConfigOptionsDocGenerator` scans the `ConfigOptions` class and generates an MDX partial file containing categorized tables of all available configuration settings.

### How it works
1. It uses reflection to find all `ConfigOption` fields in the `ConfigOptions` class.
2. It groups options into sections based on the `@ConfigSection` annotation or key prefixes.
3. It handles special default value formatting via `@ConfigOverrideDefault`.
4. It outputs an MDX file (`_partial_config.mdx`) using React-compatible HTML table syntax.

### Running the Generator

To update the configuration documentation, run the following command from the project root:

```bash
./mvnw compile -pl fluss-docgen -am && ./mvnw exec:java -pl fluss-docgen -Dexec.mainClass="org.apache.fluss.docs.ConfigOptionsDocGenerator"