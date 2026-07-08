# Fluss C++ CMake Usage Guide (System / Build Modes)

## Audience

- C++ application teams building `bindings/cpp` with CMake
- Maintainers evolving Fluss C++ dependency provisioning

## Scope

- Build system covered by this document: **CMake**
- Dependency modes covered by this document: **system/build**

Current tested baselines:

- `protoc`: `3.25.5`
- `arrow-cpp`: `19.0.1`

Notes:

- CMake currently warns (does not fail) when local `protoc`/Arrow versions differ from the baselines.
- `protoc` is required because Rust `prost-build` runs during the C++ build.

## Common Prerequisites

- Rust toolchain (`cargo` in `PATH`, or set `CARGO=/path/to/cargo`)
- `protoc` in `PATH` (required for `system` mode; `build` mode can auto-download via `bindings/cpp/scripts/ensure_protoc.sh`)
- C++17 compiler
- CMake 3.22+

Examples below use `bindings/cpp` as the source directory.

## Mode 1: `system`

Use this mode when the environment already provides Arrow C++.

### Configure

```bash
cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-system \
  -DFLUSS_CPP_DEP_MODE=system \
  -DFLUSS_CPP_ARROW_SYSTEM_ROOT=/path/to/arrow/prefix
```

Typical prefixes:

- Ubuntu package install: `/usr`
- Custom install prefix: `/usr/local` or `/opt/arrow`

### Build

```bash
cmake --build /tmp/fluss-cpp-cmake-system --target fluss_cpp -j
```

## Mode 2: `build`

Use this mode when Arrow C++ is not preinstalled and CMake should fetch/build it.

### Configure (with auto-downloaded `protoc`)

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
export PATH="$(dirname "$PROTOC_BIN"):$PATH"
```

Then configure:

```bash
cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-build \
  -DFLUSS_CPP_DEP_MODE=build
```

Optional overrides:

- `-DFLUSS_CPP_ARROW_VERSION=19.0.1`
- `-DFLUSS_CPP_ARROW_SOURCE_URL=...` (internal mirror or pinned archive)
- `-DFLUSS_CPP_PROTOBUF_VERSION=3.25.5` (baseline warning only)

If your environment needs a proxy for CMake/FetchContent downloads, export standard proxy vars before configure/build:

```bash
export http_proxy=http://host:port
export https_proxy=http://host:port
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
```

### Build

```bash
cmake --build /tmp/fluss-cpp-cmake-build --target fluss_cpp -j
```

This mode is slower on first build because it compiles Arrow C++ from source.

## Repository-local Validation (Direct Commands)

### Validate `system` mode

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
export PATH="$(dirname "$PROTOC_BIN"):$PATH"
cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-system \
  -DFLUSS_CPP_DEP_MODE=system \
  -DFLUSS_CPP_ARROW_SYSTEM_ROOT=/tmp/fluss-system-arrow-19.0.1
cmake --build /tmp/fluss-cpp-cmake-system --target fluss_cpp -j
```

### Validate `build` mode

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
export PATH="$(dirname "$PROTOC_BIN"):$PATH"
cmake -S bindings/cpp -B /tmp/fluss-cpp-cmake-build \
  -DFLUSS_CPP_DEP_MODE=build
cmake --build /tmp/fluss-cpp-cmake-build --target fluss_cpp -j
```

## Troubleshooting

- `cargo not found`
  - Install Rust toolchain or set `CARGO=/path/to/cargo`.
- `protoc not found`
  - Install `protoc` and ensure it is in `PATH`.
  - For `build` mode, use `bindings/cpp/scripts/ensure_protoc.sh` and prepend the returned path to `PATH`.
- `arrow/c/bridge.h` not found (build mode)
  - Reconfigure after updating to the latest `bindings/cpp/CMakeLists.txt`; build mode now adds Arrow source/build include dirs explicitly.
- Long first build in `build` mode
  - Expected. Arrow C++ source build dominates wall time.
