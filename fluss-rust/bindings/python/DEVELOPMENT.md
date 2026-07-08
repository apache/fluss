# Development

## Requirements

- Python 3.9+
- Rust 1.70+
- [uv](https://docs.astral.sh/uv/) package manager
- Linux or MacOS

> **Before you start:**
> Please make sure you can successfully build and run the [Fluss Rust client](../../crates/fluss/README.md) on your machine.
> The Python bindings require a working Fluss Rust backend and compatible environment.

## Install Development Dependencies

```bash
cd bindings/python
uv sync --all-extras
```

## Build Development Version

```bash
source .venv/bin/activate
uv run maturin develop
```

## Build Release Version

```bash
uv run maturin build --release
```

## Code Formatting and Linting

```bash
uv run ruff format python/
uv run ruff check python/
```

## Type Checking

```bash
uv run mypy python/
```

## Stub Drift Check

The public API is typed in `fluss/__init__.pyi`. Because that stub is
hand-written while the runtime classes come from the compiled PyO3 module, the
two can drift apart. `stubtest` (shipped with `mypy`) compares them and is run
in CI. After changing the binding's public surface, rebuild the module and run:

```bash
uv sync --extra dev --no-install-project
uv run --no-sync maturin develop --uv
uv run --no-sync python -m mypy.stubtest fluss \
  --mypy-config-file pyproject.toml \
  --allowlist stubtest-allowlist.txt
```

`stubtest-allowlist.txt` holds the few differences that are inherent to PyO3 and
cannot be expressed in the stub. Keep it minimal — stubtest treats an unused
allowlist entry as an error, so stale lines must be removed.

## Run Examples

Each example is standalone and runnable on its own. They default to a local
cluster at `127.0.0.1:9123`; override with `FLUSS_BOOTSTRAP_SERVERS`.

```bash
uv run python example/log_table.py
uv run python example/pk_table.py
uv run python example/complex_types.py
uv run python example/partitioned_table.py
uv run python example/partitioned_kv_table.py

# Point at a specific cluster:
FLUSS_BOOTSTRAP_SERVERS=host:port uv run python example/log_table.py
```

CI runs every example against an ephemeral test cluster via
`test/test_examples.py`, which auto-discovers any `example/*.py` exposing a
callable `main(bootstrap_servers)`. New examples are checked automatically with
no test changes.

## Build API Docs

```bash
uv run pdoc fluss
```

## Release

```bash
# Build wheel
uv run maturin build --release

# Publish to PyPI
uv run maturin publish
```

## Project Structure

```
bindings/python/
├── Cargo.toml            # Rust dependency configuration
├── pyproject.toml         # Python project configuration
├── README.md              # User guide
├── DEVELOPMENT.md         # This file
├── API_REFERENCE.md       # API reference
├── src/                   # Rust source code (PyO3 bindings)
│   ├── lib.rs
│   ├── config.rs
│   ├── connection.rs
│   ├── admin.rs
│   ├── table.rs
│   └── error.rs
├── fluss/                 # Python package
│   ├── __init__.py
│   ├── __init__.pyi       # Type stubs
│   └── py.typed
├── example/                       # Standalone, CI-checked examples
│   ├── log_table.py
│   ├── pk_table.py
│   ├── complex_types.py
│   ├── partitioned_table.py
│   └── partitioned_kv_table.py
└── test/
    └── test_examples.py           # Runs every example against the cluster
```

## License

Apache 2.0 License
