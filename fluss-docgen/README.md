# Apache Fluss Documentation Generator

This module contains utilities to automatically generate documentation parts from the Fluss source code. This ensures that the documentation stays in sync with the actual implementation, default values, and configuration types.

## Configuration Options Generator

The `ConfigOptionsDocGenerator` scans the `ConfigOptions` class and generates an MDX partial file containing categorized documentation of all available settings.

### How it works
1. MDX Partial Strategy: the tool produces a reusable _partial_config.mdx file. This allows Docusaurus to treat the configuration list as a React component that can be embedded anywhere.

2. Logical Grouping: Uses the @Documentation.Section annotation to categorize options into sections (e.g., Client, Server, Tablet Server) that match the existing Fluss documentation hierarchy.

3. Environment-Agnostic Defaults: Implements @Documentation.OverrideDefault to replace system-specific values (like local /tmp paths) with documentation-friendly placeholders.

4. Type Awareness: Automatically extracts and formats complex types such as Duration, MemorySize, and Boolean.

5. JSX Safety: Handles character escaping for symbols like { and < to prevent React hydration errors during the website build.

6. Output: It produces an MDX partial file (`_partial_config.mdx`) with unique anchors (`{#key-name}`) to the `website/docs/_configs/` directory.

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