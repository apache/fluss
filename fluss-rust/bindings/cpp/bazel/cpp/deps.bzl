# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Bzlmod extension for fluss C++ SDK dependency provisioning."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

_ARROW_BUILD_FILE_TEMPLATE = """
load("@rules_foreign_cc//foreign_cc:defs.bzl", "cmake")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "all_srcs",
    srcs = glob(
        ["**"],
        exclude = [
            "**/BUILD",
            "**/BUILD.bazel",
        ],
    ),
)

cmake(
    name = "arrow_cpp",
    lib_source = ":all_srcs",
    working_directory = "cpp",
    generate_args = ["-GUnix Makefiles"],
    cache_entries = {
        "CMAKE_BUILD_TYPE": "Release",
        "CMAKE_INSTALL_LIBDIR": "lib",
        "CMAKE_POSITION_INDEPENDENT_CODE": "ON",
        "ARROW_BUILD_SHARED": "ON",
        "ARROW_BUILD_STATIC": "OFF",
        "ARROW_BUILD_TESTS": "OFF",
        "ARROW_BUILD_EXAMPLES": "OFF",
        "ARROW_BUILD_BENCHMARKS": "OFF",
        "ARROW_BUILD_INTEGRATION": "OFF",
        "ARROW_BUILD_UTILITIES": "OFF",
        "ARROW_COMPUTE": "OFF",
        "ARROW_CSV": "OFF",
        "ARROW_DATASET": "OFF",
        "ARROW_FILESYSTEM": "OFF",
        "ARROW_JSON": "OFF",
        "ARROW_PARQUET": "OFF",
        "ARROW_IPC": "ON",
        "ARROW_JEMALLOC": "OFF",
        "ARROW_MIMALLOC": "OFF",
        "ARROW_SIMD_LEVEL": "NONE",
        "ARROW_RUNTIME_SIMD_LEVEL": "NONE",
        "ARROW_DEPENDENCY_SOURCE": "BUNDLED",
        # Temporary workarounds for older images / Bazel sandbox toolchain detection.
        "EP_CMAKE_RANLIB": "__EP_CMAKE_RANLIB__",
        "EP_CMAKE_AR": "__EP_CMAKE_AR__",
        "EP_CMAKE_NM": "__EP_CMAKE_NM__",
    },
    out_include_dir = "include",
    out_lib_dir = "lib",
    out_shared_libs = select({
        "@platforms//os:macos": [
            "libarrow.dylib",
            "libarrow.1900.dylib",
        ],
        "//conditions:default": [
            "libarrow.so",
            "libarrow.so.1900",
            "libarrow.so.1900.1.0",
        ],
    }),
)
"""

_ARROW_PATCH_CMDS = [
    "sed -i.bak 's|#define ARROW_CXX_COMPILER_FLAGS \"@CMAKE_CXX_FLAGS@\"|#define ARROW_CXX_COMPILER_FLAGS \"\"|' cpp/src/arrow/util/config.h.cmake && rm -f cpp/src/arrow/util/config.h.cmake.bak",
]

_SYSTEM_ARROW_BUILD_FILE_TEMPLATE = """
load("@rules_cc//cc:defs.bzl", "cc_import", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_import(
    name = "arrow_shared_import",
    shared_library = "__SYSTEM_ARROW_SHARED_LIBRARY__",
)

filegroup(
    name = "arrow_runtime_libs",
    srcs = [
__SYSTEM_ARROW_RUNTIME_SRCS__
    ],
)

cc_library(
    name = "arrow_cpp",
    hdrs = [
__SYSTEM_ARROW_HDRS__
    ],
    includes = ["__SYSTEM_ARROW_INCLUDE_DIR__"],
    data = [":arrow_runtime_libs"],
    deps = [":arrow_shared_import"],
)
"""

_ARROW_BUILD_VERSIONS = {
    "19.0.1": {
        "urls": ["https://github.com/apache/arrow/archive/refs/tags/apache-arrow-19.0.1.tar.gz"],
        "strip_prefix": "arrow-apache-arrow-19.0.1",
        "integrity": "sha256-TImFBJWIQcyGtvhxDsspGflrXhD6iYmsEKxPyoNi2Go=",
    },
}

_config_tag = tag_class(attrs = {
    "mode": attr.string(default = "build"),
    "arrow_cpp_version": attr.string(default = "19.0.1"),
    "protobuf_version": attr.string(default = "3.25.5"),
    "ep_cmake_ranlib": attr.string(default = "ranlib"),
    "ep_cmake_ar": attr.string(default = "ar"),
    "ep_cmake_nm": attr.string(default = "nm"),
    "system_arrow_prefix": attr.string(default = "/usr"),
    "system_arrow_include_dir": attr.string(default = "include"),
    "system_arrow_shared_library": attr.string(default = "lib/x86_64-linux-gnu/libarrow.so"),
    "system_arrow_runtime_glob": attr.string(default = "lib/x86_64-linux-gnu/libarrow.so*"),
})

def _render_arrow_build_file(tag):
    return _ARROW_BUILD_FILE_TEMPLATE.replace(
        "__EP_CMAKE_RANLIB__",
        tag.ep_cmake_ranlib,
    ).replace(
        "__EP_CMAKE_AR__",
        tag.ep_cmake_ar,
    ).replace(
        "__EP_CMAKE_NM__",
        tag.ep_cmake_nm,
    )

def _render_system_arrow_build_file(tag, shared_library_override = None):
    shared_library = shared_library_override if shared_library_override else (tag.system_arrow_shared_library if hasattr(tag, "system_arrow_shared_library") else tag.shared_library)
    include_dir = tag.system_arrow_include_dir if hasattr(tag, "system_arrow_include_dir") else tag.include_dir
    return _SYSTEM_ARROW_BUILD_FILE_TEMPLATE.replace(
        "__SYSTEM_ARROW_SHARED_LIBRARY__",
        "sysroot/" + shared_library,
    ).replace(
        "__SYSTEM_ARROW_INCLUDE_DIR__",
        "sysroot/" + include_dir,
    )

def _starlark_string_list(items):
    if not items:
        return ""
    return "\n".join(['        "%s",' % i for i in items])

def _list_files(repo_ctx, base_dir, suffixes):
    result = repo_ctx.execute([
        "/usr/bin/find",
        base_dir,
        "(",
        "-type",
        "f",
        "-o",
        "-type",
        "l",
        ")",
    ])
    if result.return_code != 0:
        fail("failed to enumerate files under %s: %s" % (base_dir, result.stderr))
    files = []
    for line in result.stdout.splitlines():
        for suffix in suffixes:
            if line.endswith(suffix):
                files.append(line)
                break
    return sorted(files)

def _copy_file_to_sysroot(repo_ctx, prefix, rel_path):
    if rel_path.startswith("/"):
        fail("expected relative path under prefix, got absolute path: %s" % rel_path)
    src = prefix + "/" + rel_path
    dst = "sysroot/" + rel_path
    dst_parent = dst.rsplit("/", 1)[0] if "/" in dst else "sysroot"
    mkdir_res = repo_ctx.execute(["/bin/mkdir", "-p", dst_parent])
    if mkdir_res.return_code != 0:
        fail("failed to create directory %s: %s" % (dst_parent, mkdir_res.stderr))
    # Resolve symlinks into real files to keep the generated sysroot self-contained.
    cp_res = repo_ctx.execute(["/bin/cp", "-L", src, dst])
    if cp_res.return_code != 0:
        fail("failed to copy %s to %s: %s" % (src, dst, cp_res.stderr))

def _system_arrow_repo_impl(repo_ctx):
    prefix = repo_ctx.attr.prefix.rstrip("/")
    include_dir = repo_ctx.attr.include_dir
    shared_library = repo_ctx.attr.shared_library
    runtime_glob = repo_ctx.attr.runtime_glob

    mkdir_res = repo_ctx.execute(["/bin/mkdir", "-p", "sysroot"])
    if mkdir_res.return_code != 0:
        fail("failed to create sysroot directory: %s" % mkdir_res.stderr)

    include_dir_for_scan = include_dir
    if include_dir_for_scan.endswith("/"):
        include_dir_for_scan = include_dir_for_scan[:-1]
    header_root = prefix + "/" + include_dir_for_scan + "/arrow"
    headers = _list_files(repo_ctx, header_root, [".h", ".hpp"])
    header_srcs_rel = []
    header_srcs = []
    for h in headers:
        if not h.startswith(prefix + "/"):
            fail("header path %s is outside prefix %s" % (h, prefix))
        rel = h[len(prefix) + 1:]
        header_srcs_rel.append(rel)
        header_srcs.append("sysroot/" + rel)

    runtime_dir = runtime_glob.rsplit("/", 1)[0]
    runtime_prefix = runtime_glob.rsplit("/", 1)[1].replace("*", "")
    runtime_files = _list_files(repo_ctx, prefix + "/" + runtime_dir, [""])
    runtime_srcs_rel = []
    runtime_srcs = []
    for f in runtime_files:
        rel = f[len(prefix) + 1:] if f.startswith(prefix + "/") else None
        if rel == None:
            continue
        if rel.startswith(runtime_dir + "/") and rel.rsplit("/", 1)[1].startswith(runtime_prefix):
            runtime_srcs_rel.append(rel)
            runtime_srcs.append("sysroot/" + rel)
    runtime_srcs_rel = sorted(runtime_srcs_rel)
    runtime_srcs = sorted(runtime_srcs)

    # Prefer a versioned soname file as the imported shared library so Bazel
    # runfiles contain the exact filename required by the runtime loader.
    shared_import_rel = "sysroot/" + shared_library
    shared_basename = shared_library.rsplit("/", 1)[1]
    soname_candidates = []
    for rel in runtime_srcs_rel:
        base = rel.rsplit("/", 1)[1]
        if base == shared_basename:
            continue
        if base.startswith(shared_basename + "."):
            soname_candidates.append("sysroot/" + rel)
    if soname_candidates:
        # Prefer shortest suffix first (e.g. libarrow.so.1900 before
        # libarrow.so.1900.1.0) to match ELF SONAME naming when available.
        soname_candidates = sorted(soname_candidates, key = lambda s: (len(s), s))
        shared_import_rel = soname_candidates[0]

    # Copy only required Arrow artifacts instead of mirroring the full system prefix.
    copy_rel_paths = {}
    for rel in header_srcs_rel + runtime_srcs_rel + [shared_library]:
        copy_rel_paths[rel] = True
    for rel in sorted(copy_rel_paths.keys()):
        _copy_file_to_sysroot(repo_ctx, prefix, rel)

    build_file = _render_system_arrow_build_file(repo_ctx.attr, shared_library_override = shared_import_rel[len("sysroot/"):]).replace(
        "__SYSTEM_ARROW_HDRS__",
        _starlark_string_list(header_srcs),
    ).replace(
        "__SYSTEM_ARROW_RUNTIME_SRCS__",
        _starlark_string_list(runtime_srcs),
    )
    repo_ctx.file("BUILD.bazel", build_file)

_system_arrow_repository = repository_rule(
    implementation = _system_arrow_repo_impl,
    attrs = {
        "prefix": attr.string(mandatory = True),
        "include_dir": attr.string(mandatory = True),
        "shared_library": attr.string(mandatory = True),
        "runtime_glob": attr.string(mandatory = True),
    },
    local = True,
)

def _select_config(ctx):
    selected = None
    selected_owner = None
    root_selected = None
    for mod in ctx.modules:
        for tag in mod.tags.config:
            is_root = hasattr(mod, "is_root") and mod.is_root
            if is_root:
                if root_selected != None:
                    fail("cpp_sdk.config may only be declared once in the root module")
                root_selected = tag
                continue
            if selected == None:
                selected = tag
                selected_owner = mod.name
            elif selected_owner != mod.name:
                # Prefer root override. Dependency defaults are tolerated as long
                # as they come from a single module.
                fail("multiple dependency defaults for cpp_sdk.config without root override")
    if root_selected != None:
        return root_selected
    return selected

def _cpp_sdk_impl(ctx):
    tag = _select_config(ctx)
    if tag == None:
        return

    if tag.mode == "registry":
        return

    if tag.mode == "system":
        _system_arrow_repository(
            name = "apache_arrow_cpp",
            prefix = tag.system_arrow_prefix,
            include_dir = tag.system_arrow_include_dir,
            shared_library = tag.system_arrow_shared_library,
            runtime_glob = tag.system_arrow_runtime_glob,
        )
        return

    if tag.mode != "build":
        fail("unsupported cpp_sdk mode: %s" % tag.mode)

    arrow_version = _ARROW_BUILD_VERSIONS.get(tag.arrow_cpp_version)
    if arrow_version == None:
        fail("unsupported arrow_cpp_version for build mode: %s" % tag.arrow_cpp_version)

    http_archive(
        name = "apache_arrow_cpp",
        urls = arrow_version["urls"],
        strip_prefix = arrow_version["strip_prefix"],
        integrity = arrow_version["integrity"],
        patch_cmds = _ARROW_PATCH_CMDS,
        build_file_content = _render_arrow_build_file(tag),
    )

cpp_sdk = module_extension(
    implementation = _cpp_sdk_impl,
    tag_classes = {
        "config": _config_tag,
    },
)
