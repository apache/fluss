# Fluss Documentation Generator

This module contains utilities to automatically generate documentation parts from the Fluss source code. This ensures that the documentation stays in sync with the actual implementation, default values, and configuration types.

## Configuration Options Generator

The `ConfigOptionsDocGenerator` scans the `ConfigOptions` class and generates an MDX partial file containing categorized documentation of all available settings.

### How it works
1. It uses reflection to find all `ConfigOption` fields in the `ConfigOptions` class.
2. It groups options into sections based on the `@ConfigSection` annotation or key prefixes (Server, Client, Table).
3. It handles special default value formatting via `@ConfigOverrideDefault`.
4. It cleanses descriptions (escaping characters like `{` or `<`) and normalizes types (e.g., `Int`, `Duration`, `MemorySize`) for MDX compatibility.
5. It outputs an MDX partial file (`_partial_config.mdx`) with unique anchors (`{#key-name}`) to the `website/docs/_configs/` directory.

### Running the Generator

To update the configuration documentation, run the following command from the project root:

```bash
./mvnw compile -pl fluss-docgen -am
```

## Integration with Website

The generated file is stored in `website/docs/_configs/_partial_config.mdx`. To display the documentation, use the MDX import syntax in any `.md` or `.mdx` file:

```markdown
import PartialConfig from '../_configs/_partial_config.mdx';
<PartialConfig></PartialConfig>
```