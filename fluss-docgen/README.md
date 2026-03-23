# Apache Fluss Documentation Generator

This module contains utilities to automatically generate documentation from the
Fluss source code, ensuring that the documentation stays in sync with the actual
implementation, default values, and configuration types.

## Configuration Options Generator

The `ConfigOptionsDocGenerator` scans the `ConfigOptions` class and generates an
MDX partial file containing categorized documentation of all available settings.

### How it works

1. **MDX Partial Strategy**: Produces a reusable `_partial_config.mdx` file that
   Docusaurus treats as a React component, embeddable in any documentation page.

2. **Logical Grouping**: Uses the `@Documentation.Section` annotation to categorize
   options into sections (e.g., `Client`, `Server`, `ZooKeeper`) that match the
   existing Fluss documentation hierarchy. Options without a `@Documentation.Section`
   annotation are intentionally excluded from the generated output.

3. **Environment-Agnostic Defaults**: Uses the `@Documentation.OverrideDefault`
   annotation to replace system-specific values (such as local `/tmp` paths or
   system timezone strings) with documentation-friendly placeholders.

4. **Type Awareness**: Automatically extracts and formats complex types such as
   `Duration`, `MemorySize`, `Boolean`, and enum types. Sub-second `Duration`
   values are formatted as milliseconds (e.g., `100 ms`). Near-infinite values
   for `Duration` and `MemorySize` are rendered as `infinite`.

5. **JSX Safety**: Handles character escaping for symbols like `{`, `}`, and `<`,
   and strips Markdown link syntax to prevent React hydration errors during the
   website build.

6. **Deprecated Options**: Options annotated with `@Deprecated` are rendered with
   a visible deprecation notice and still appear in the documentation for reference.

7. **Output**: Produces `website/docs/_configs/_partial_config.mdx` with unique
   section anchors (e.g., `{#key-name}`) for deep linking.

### Running the Generator

To regenerate the configuration documentation, run the following command from the
project root:
```bash
./mvnw compile -pl fluss-docgen -am
```

The updated file will be written to `website/docs/_configs/_partial_config.mdx`.

### Keeping Documentation in Sync

When adding or modifying a `ConfigOption` in `ConfigOptions.java`, follow these steps
to ensure it appears correctly in the documentation:

1. **Assign a section** by adding `@Documentation.Section("SectionName")` above the
   field. Options without this annotation are excluded from the generated output.

2. **Override system-dependent defaults** by adding `@Documentation.OverrideDefault("value")`
   above the field. This is required for options whose default value is derived at
   runtime (e.g., `System.getProperty("java.io.tmpdir")`, `ZoneId.systemDefault()`,
   `Long.MAX_VALUE`, `Integer.MAX_VALUE`).

3. **Add a description** via `.withDescription(...)` on the `ConfigOption` builder.
   Options with no description will render `(No description provided.)` in the docs.

4. **Re-run the generator** using the command above to regenerate `_partial_config.mdx`.

Example of a well-annotated option:
```java
@Documentation.Section("Client")
@Documentation.OverrideDefault("/tmp/fluss")
public static final ConfigOption<String> CLIENT_SCANNER_IO_TMP_DIR =
        key("client.scanner.io.tmpdir")
                .stringType()
                .defaultValue(System.getProperty("java.io.tmpdir") + "/fluss")
                .withDescription(
                        "Local directory used by the client for storing data files "
                                + "(such as kv snapshot and log segment files) temporarily.");
```

## Integration with Website

The generated file is stored at `website/docs/_configs/_partial_config.mdx`.
To display the documentation in any `.md` or `.mdx` page, use the MDX import syntax:
```markdown
import PartialConfig from '../_configs/_partial_config.mdx';

<PartialConfig />
```

> **Note**: Adjust the relative import path based on the location of the importing
> file within the `website/docs/` directory. For example, a file located at
> `website/docs/maintenance/configuration.md` uses `../_configs/_partial_config.mdx`.
