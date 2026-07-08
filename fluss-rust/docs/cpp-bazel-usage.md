# Fluss C++ Bazel Usage Guide (System / Build Modes)

This guide is for:

- C++ application teams consuming Fluss C++ bindings via Bazel
- Maintainers evolving the Bazel integration

For the CMake flow with the same `system` / `build` dependency modes, see
`docs/cpp-cmake-usage.md`.

Current simplification scope:

- Keep only two dependency modes in the mainline guidance:
  - `system`
  - `build`
- Defer strict internal-registry-only module flow from the mainline path

## Scope

- Dependency model: **root module mode**
- Consumer dependency target: `@fluss-cpp//bindings/cpp:fluss_cpp`
- Root `MODULE.bazel` is required for root module mode.
- Build systems covered by this document: **Bazel**
- Dependency modes covered by this document: **system/build**

Version baseline references currently used by examples:

- `protobuf/protoc`: `3.25.5`
- `arrow-cpp`: `19.0.1`

## Common Consumer `BUILD.bazel`

Both modes use the same dependency target:

```starlark
load("@rules_cc//cc:defs.bzl", "cc_binary")

cc_binary(
    name = "fluss_reader",
    srcs = ["reader.cc"],
    deps = ["@fluss-cpp//bindings/cpp:fluss_cpp"],
)
```

## Mode 1: `system` (Recommended in preinstalled environments)

Use this mode when your environment already provides:

- `protoc`
- Arrow C++ (headers + shared libraries)

### Consumer `MODULE.bazel` (pattern)

```starlark
module(name = "my_cpp_app")

bazel_dep(name = "rules_cc", version = "0.2.14")
bazel_dep(name = "fluss-cpp", version = "<released-version>")

fluss_cpp = use_extension("@fluss-cpp//bindings/cpp/bazel/cpp:deps.bzl", "cpp_sdk")
fluss_cpp.config(
    mode = "system",
    protobuf_version = "3.25.5",
    arrow_cpp_version = "19.0.1",
    # Adjust Arrow paths for your environment
    system_arrow_prefix = "/usr",
    system_arrow_include_dir = "include",
    system_arrow_shared_library = "lib/x86_64-linux-gnu/libarrow.so",
    system_arrow_runtime_glob = "lib/x86_64-linux-gnu/libarrow.so*",
)
use_repo(fluss_cpp, "apache_arrow_cpp")
```

### Build and run (consumer workspace pattern)

Run from your consumer workspace root (the directory containing
`MODULE.bazel` and your top-level `BUILD.bazel`).

```bash
PROTOC_BIN="$(command -v protoc)"
CARGO_BIN="$(command -v cargo)"
bazel run \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  //:fluss_reader
```

### Runnable example

- `bindings/cpp/examples/bazel-consumer/system`

```bash
cd bindings/cpp/examples/bazel-consumer/system
PROTOC_BIN="$(command -v protoc)"
CARGO_BIN="$(command -v cargo)"
bazel run \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  //:consumer_system
```

## Mode 2: `build` (No internal registry / no preinstalled Arrow)

Use this mode when Arrow C++ is not preinstalled and you want Bazel to
provision it from source.

### Consumer `MODULE.bazel` (pattern)

```starlark
module(name = "my_cpp_app")

bazel_dep(name = "rules_cc", version = "0.2.14")
bazel_dep(name = "fluss-cpp", version = "<released-version>")

fluss_cpp = use_extension("@fluss-cpp//bindings/cpp/bazel/cpp:deps.bzl", "cpp_sdk")
fluss_cpp.config(
    mode = "build",
    protobuf_version = "3.25.5",
    arrow_cpp_version = "19.0.1",
)
use_repo(fluss_cpp, "apache_arrow_cpp")
```

Notes:

- `build` mode in the core Bazel integration still uses `PROTOC` (env / PATH).
- To auto-download a pinned `protoc` for `build` mode, use
  `bindings/cpp/scripts/ensure_protoc.sh` and pass the result via `--action_env=PROTOC=...`.
- `ensure_protoc.sh` auto-detects host OS/arch (`linux`/`osx`, `x86_64`/`aarch_64`).
- Some environments may require `ep_cmake_ar/ranlib/nm` overrides.

### Build and run (consumer workspace pattern, with auto-downloaded `protoc`)

Run from the `fluss-rust` repository root, or adjust the script path if you
copied it elsewhere.

```bash
PROTOC_BIN="$(bash bindings/cpp/scripts/ensure_protoc.sh --print-path)"
```

```bash
bazel run --action_env=PROTOC="$PROTOC_BIN" //:fluss_reader
```

If `cargo` is not on Bazel action `PATH`, also pass:

```bash
CARGO_BIN="$(command -v cargo)"
bazel run \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  //:fluss_reader
```

### Runnable example

- `bindings/cpp/examples/bazel-consumer/build`

```bash
cd bindings/cpp/examples/bazel-consumer/build
PROTOC_BIN="$(bash ../../../scripts/ensure_protoc.sh --print-path)"
CARGO_BIN="$(command -v cargo)"
bazel run \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  //:consumer_build
```

## Local Development Override (Optional)

For repository-local validation only:

```starlark
local_path_override(
    module_name = "fluss-cpp",
    path = "/path/to/fluss-rust",
)
```

Do not keep local overrides in long-lived branches.

Repository-local examples in this repo use `version = "0.1.0"` together with
`local_path_override(...)` for local validation before publishing to the Bazel
registry.

## Repository-local Validation (Direct Commands)

These commands validate the repository examples directly.
If your environment requires a proxy for Bazel external downloads, export it
before running (replace the placeholder URL with your actual proxy):

```bash
export BAZEL_PROXY_URL="http://proxy.example.com:3128"
export http_proxy="$BAZEL_PROXY_URL"
export https_proxy="$BAZEL_PROXY_URL"
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
unset all_proxy ALL_PROXY
```

### Validate `build` example

```bash
cd bindings/cpp/examples/bazel-consumer/build
PROTOC_BIN="$(bash ../../../scripts/ensure_protoc.sh --print-path)"
CARGO_BIN="$(command -v cargo)"
bazel --ignore_all_rc_files run \
  --registry=https://bcr.bazel.build \
  --lockfile_mode=off \
  --repo_env=http_proxy="${http_proxy:-}" \
  --repo_env=https_proxy="${https_proxy:-}" \
  --repo_env=HTTP_PROXY="${HTTP_PROXY:-}" \
  --repo_env=HTTPS_PROXY="${HTTPS_PROXY:-}" \
  --action_env=http_proxy="${http_proxy:-}" \
  --action_env=https_proxy="${https_proxy:-}" \
  --action_env=HTTP_PROXY="${HTTP_PROXY:-}" \
  --action_env=HTTPS_PROXY="${HTTPS_PROXY:-}" \
  --action_env=all_proxy= \
  --action_env=ALL_PROXY= \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  --strategy=CcCmakeMakeRule=local \
  --strategy=BootstrapGNUMake=local \
  --strategy=BootstrapPkgConfig=local \
  //:consumer_build
```

### Validate `system` example (using a local Arrow prefix)

The `system` example defaults to `/usr`. If your Arrow prefix is elsewhere
(for example a locally built prefix), copy the example to a temp directory and
patch `MODULE.bazel` before running:

```bash
tmp_dir="$(mktemp -d /tmp/fluss-bazel-system-doc.XXXXXX)"
FLUSS_RUST_ROOT="$(pwd)"
cp -a bindings/cpp/examples/bazel-consumer/system/. "$tmp_dir/"
sed -i \
  -e "s|path = \"../../../../../\"|path = \"$FLUSS_RUST_ROOT\"|" \
  -e 's|system_arrow_prefix = "/usr"|system_arrow_prefix = "/tmp/fluss-system-arrow-19.0.1"|' \
  -e 's|system_arrow_shared_library = "lib/x86_64-linux-gnu/libarrow.so"|system_arrow_shared_library = "lib/libarrow.so"|' \
  -e 's|system_arrow_runtime_glob = "lib/x86_64-linux-gnu/libarrow.so\\*"|system_arrow_runtime_glob = "lib/libarrow.so*"|' \
  "$tmp_dir/MODULE.bazel"
cd "$tmp_dir"
PROTOC_BIN="$(command -v protoc)"
CARGO_BIN="$(command -v cargo)"
bazel --ignore_all_rc_files run \
  --registry=https://bcr.bazel.build \
  --lockfile_mode=off \
  --repo_env=http_proxy="${http_proxy:-}" \
  --repo_env=https_proxy="${https_proxy:-}" \
  --repo_env=HTTP_PROXY="${HTTP_PROXY:-}" \
  --repo_env=HTTPS_PROXY="${HTTPS_PROXY:-}" \
  --action_env=http_proxy="${http_proxy:-}" \
  --action_env=https_proxy="${https_proxy:-}" \
  --action_env=HTTP_PROXY="${HTTP_PROXY:-}" \
  --action_env=HTTPS_PROXY="${HTTPS_PROXY:-}" \
  --action_env=all_proxy= \
  --action_env=ALL_PROXY= \
  --action_env=PROTOC="$PROTOC_BIN" \
  --action_env=CARGO="$CARGO_BIN" \
  --action_env=PATH="$(dirname "$CARGO_BIN"):$PATH" \
  //:consumer_system
```

On macOS (BSD `sed`), replace `sed -i` with `sed -i ''` in the patch step above.

## Upgrade Procedure

1. Update `bazel_dep(name = "fluss-cpp", version = "...")`
2. Update mode version settings if needed (`protobuf_version`, `arrow_cpp_version`)
3. Run `bazel mod tidy`
4. Commit `MODULE.bazel` and `MODULE.bazel.lock`
5. Run build + tests
6. Verify dependency graph:

```bash
bazel mod graph | rg "fluss-cpp@"
```

## Examples and Non-Mainline References

Mainline examples:

- `bindings/cpp/examples/bazel-consumer/build`
- `bindings/cpp/examples/bazel-consumer/system`
