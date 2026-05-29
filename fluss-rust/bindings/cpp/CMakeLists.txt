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

cmake_minimum_required(VERSION 3.22)

if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif()

project(fluss-cpp LANGUAGES CXX)

include(FetchContent)
set(FLUSS_GOOGLETEST_VERSION 1.15.2 CACHE STRING "version of GoogleTest")
set(FLUSS_NLOHMANN_JSON_VERSION 3.12.0 CACHE STRING "version of nlohmann/json")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(FLUSS_CPP_DEP_MODE "system" CACHE STRING "Dependency provisioning mode for fluss-cpp (system|build)")
set_property(CACHE FLUSS_CPP_DEP_MODE PROPERTY STRINGS system build)
set(FLUSS_CPP_ARROW_VERSION "19.0.1" CACHE STRING "Arrow C++ version baseline for fluss-cpp")
set(FLUSS_CPP_PROTOBUF_VERSION "3.25.5" CACHE STRING "Protobuf/protoc version baseline for fluss-cpp")
set(FLUSS_CPP_ARROW_SYSTEM_ROOT "" CACHE PATH "Optional Arrow installation prefix for system mode")
set(FLUSS_CPP_ARROW_SOURCE_URL
    "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-19.0.1.tar.gz"
    CACHE STRING
    "Arrow source archive URL used in build mode")
set(FLUSS_CPP_ARROW_SOURCE_SHA256
    "4c898504958841cc86b6f8710ecb2919f96b5e10fa8989ac10ac4fca8362d86a"
    CACHE STRING
    "SHA256 for the Arrow source archive used in build mode")

find_package(Threads REQUIRED)

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(FLUSS_ENABLE_ADDRESS_SANITIZER "Enable address sanitizer" OFF)
option(FLUSS_ENABLE_TESTING "Enable building test binary for fluss" OFF)
option(FLUSS_DEV "Enable dev mode" OFF)

if (FLUSS_DEV)
    set(FLUSS_ENABLE_ADDRESS_SANITIZER ON)
    set(FLUSS_ENABLE_TESTING ON)
endif()

if (NOT FLUSS_CPP_DEP_MODE STREQUAL "system" AND NOT FLUSS_CPP_DEP_MODE STREQUAL "build")
    message(FATAL_ERROR "Unsupported FLUSS_CPP_DEP_MODE='${FLUSS_CPP_DEP_MODE}'. Expected 'system' or 'build'.")
endif()

find_program(FLUSS_PROTOC_EXECUTABLE NAMES protoc)
if (NOT FLUSS_PROTOC_EXECUTABLE)
    message(FATAL_ERROR "protoc not found. Install protoc or set it in PATH. (Fluss baseline: ${FLUSS_CPP_PROTOBUF_VERSION})")
endif()

if (DEFINED ENV{CARGO} AND NOT "$ENV{CARGO}" STREQUAL "" AND EXISTS "$ENV{CARGO}")
    set(FLUSS_CARGO_EXECUTABLE "$ENV{CARGO}")
else()
    if (DEFINED ENV{CARGO} AND NOT "$ENV{CARGO}" STREQUAL "")
        get_filename_component(_FLUSS_CARGO_HINT_DIR "$ENV{CARGO}" DIRECTORY)
    endif()
    find_program(FLUSS_CARGO_EXECUTABLE NAMES cargo HINTS "${_FLUSS_CARGO_HINT_DIR}")
endif()
if (NOT FLUSS_CARGO_EXECUTABLE)
    message(FATAL_ERROR "cargo not found. Install Rust toolchain or set CARGO/PATH.")
endif()

execute_process(
    COMMAND ${FLUSS_PROTOC_EXECUTABLE} --version
    OUTPUT_VARIABLE FLUSS_PROTOC_VERSION_OUTPUT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)
string(REGEX MATCH "([0-9]+\\.[0-9]+\\.[0-9]+)" FLUSS_PROTOC_VERSION "${FLUSS_PROTOC_VERSION_OUTPUT}")
set(FLUSS_PROTOC_VERSION_NORM "${FLUSS_PROTOC_VERSION}")
set(FLUSS_CPP_PROTOBUF_VERSION_NORM "${FLUSS_CPP_PROTOBUF_VERSION}")
string(REGEX REPLACE "^3\\." "" FLUSS_PROTOC_VERSION_NORM "${FLUSS_PROTOC_VERSION_NORM}")
string(REGEX REPLACE "^3\\." "" FLUSS_CPP_PROTOBUF_VERSION_NORM "${FLUSS_CPP_PROTOBUF_VERSION_NORM}")
if (FLUSS_PROTOC_VERSION AND
    NOT FLUSS_PROTOC_VERSION VERSION_EQUAL FLUSS_CPP_PROTOBUF_VERSION AND
    NOT FLUSS_PROTOC_VERSION_NORM VERSION_EQUAL FLUSS_CPP_PROTOBUF_VERSION_NORM)
    message(WARNING
        "protoc version (${FLUSS_PROTOC_VERSION}) does not match Fluss baseline "
        "(${FLUSS_CPP_PROTOBUF_VERSION}). Build may still work, but this is outside the tested baseline.")
endif()

message(STATUS "Fluss C++ dependency mode: ${FLUSS_CPP_DEP_MODE}")
message(STATUS "Fluss C++ protoc executable: ${FLUSS_PROTOC_EXECUTABLE} (${FLUSS_PROTOC_VERSION_OUTPUT})")
message(STATUS "Fluss C++ cargo executable: ${FLUSS_CARGO_EXECUTABLE}")

if (FLUSS_CPP_DEP_MODE STREQUAL "system")
    if (FLUSS_CPP_ARROW_SYSTEM_ROOT)
        list(APPEND CMAKE_PREFIX_PATH "${FLUSS_CPP_ARROW_SYSTEM_ROOT}")
        set(Arrow_ROOT "${FLUSS_CPP_ARROW_SYSTEM_ROOT}")
    endif()

    find_package(Arrow REQUIRED)

    if (DEFINED Arrow_VERSION AND Arrow_VERSION AND NOT Arrow_VERSION VERSION_EQUAL FLUSS_CPP_ARROW_VERSION)
        message(WARNING
            "Arrow version (${Arrow_VERSION}) does not match Fluss baseline "
            "(${FLUSS_CPP_ARROW_VERSION}). Build may still work, but this is outside the tested baseline.")
    endif()
else()
    # Build mode: provision Arrow C++ from source in-tree.
    set(ARROW_BUILD_SHARED ON CACHE BOOL "" FORCE)
    set(ARROW_BUILD_STATIC OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_INTEGRATION OFF CACHE BOOL "" FORCE)
    set(ARROW_BUILD_UTILITIES OFF CACHE BOOL "" FORCE)
    set(ARROW_COMPUTE OFF CACHE BOOL "" FORCE)
    set(ARROW_CSV OFF CACHE BOOL "" FORCE)
    set(ARROW_DATASET OFF CACHE BOOL "" FORCE)
    set(ARROW_FILESYSTEM OFF CACHE BOOL "" FORCE)
    set(ARROW_JSON OFF CACHE BOOL "" FORCE)
    set(ARROW_PARQUET OFF CACHE BOOL "" FORCE)
    set(ARROW_IPC ON CACHE BOOL "" FORCE)
    # Reduce third-party sub-build complexity in build mode.
    set(ARROW_JEMALLOC OFF CACHE BOOL "" FORCE)
    set(ARROW_MIMALLOC OFF CACHE BOOL "" FORCE)
    set(ARROW_DEPENDENCY_SOURCE BUNDLED CACHE STRING "" FORCE)
    set(ARROW_SIMD_LEVEL NONE CACHE STRING "" FORCE)
    set(ARROW_RUNTIME_SIMD_LEVEL NONE CACHE STRING "" FORCE)

    FetchContent_Declare(
        apache_arrow_src
        URL ${FLUSS_CPP_ARROW_SOURCE_URL}
        URL_HASH SHA256=${FLUSS_CPP_ARROW_SOURCE_SHA256}
        SOURCE_SUBDIR cpp
    )
    FetchContent_MakeAvailable(apache_arrow_src)
    set(FLUSS_CPP_ARROW_EXTRA_INCLUDE_DIRS
        "${apache_arrow_src_SOURCE_DIR}/cpp/src"
        "${apache_arrow_src_BINARY_DIR}/src")

    if (TARGET arrow_shared AND NOT TARGET Arrow::arrow_shared)
        add_library(Arrow::arrow_shared ALIAS arrow_shared)
    endif()
    if (NOT TARGET Arrow::arrow_shared)
        message(FATAL_ERROR "Arrow build mode did not produce target Arrow::arrow_shared (or arrow_shared).")
    endif()
endif()

# Get cargo target dir
execute_process(COMMAND ${FLUSS_CARGO_EXECUTABLE} locate-project --workspace --message-format plain
    OUTPUT_VARIABLE CARGO_MANIFEST_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})
if (NOT CARGO_MANIFEST_PATH)
    message(FATAL_ERROR
        "Failed to resolve Cargo workspace target dir via '${FLUSS_CARGO_EXECUTABLE} locate-project'. "
        "Check Rust toolchain installation and PATH/CARGO.")
endif()
get_filename_component(CARGO_WORKSPACE_DIR "${CARGO_MANIFEST_PATH}" DIRECTORY)
set(CARGO_TARGET_DIR "${CARGO_WORKSPACE_DIR}/target")

set(CARGO_MANIFEST ${PROJECT_SOURCE_DIR}/Cargo.toml)
set(RUST_SOURCE_FILE ${PROJECT_SOURCE_DIR}/src/lib.rs)
set(RUST_BRIDGE_CPP ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src/lib.rs.cc)
set(RUST_HEADER_FILE ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src/lib.rs.h)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(RUST_LIB ${CARGO_TARGET_DIR}/debug/${CMAKE_STATIC_LIBRARY_PREFIX}fluss_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
else()
    set(RUST_LIB ${CARGO_TARGET_DIR}/release/${CMAKE_STATIC_LIBRARY_PREFIX}fluss_cpp${CMAKE_STATIC_LIBRARY_SUFFIX})
endif()

set(CPP_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/include
                    ${PROJECT_SOURCE_DIR}/src
                    ${CARGO_TARGET_DIR}/cxxbridge
                    ${CARGO_TARGET_DIR}/cxxbridge/fluss-cpp/src)

file(GLOB CPP_SOURCE_FILE "src/*.cpp")
file(GLOB CPP_HEADER_FILE "include/*.hpp")

if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    list(APPEND CARGO_BUILD_FLAGS "--release")
endif()

add_custom_target(cargo_build
    COMMAND ${CMAKE_COMMAND} -E env PROTOC=${FLUSS_PROTOC_EXECUTABLE} ${FLUSS_CARGO_EXECUTABLE} build --manifest-path ${CARGO_MANIFEST} ${CARGO_BUILD_FLAGS}
    BYPRODUCTS ${RUST_BRIDGE_CPP} ${RUST_LIB} ${RUST_HEADER_FILE}
    DEPENDS ${RUST_SOURCE_FILE}
    USES_TERMINAL
    COMMENT "Running cargo..."
)

add_library(fluss_cpp STATIC ${CPP_SOURCE_FILE} ${RUST_BRIDGE_CPP})
target_sources(fluss_cpp PUBLIC ${CPP_HEADER_FILE})
target_sources(fluss_cpp PRIVATE ${RUST_HEADER_FILE})
target_include_directories(fluss_cpp PUBLIC ${CPP_INCLUDE_DIR})
if (FLUSS_CPP_ARROW_EXTRA_INCLUDE_DIRS)
    target_include_directories(fluss_cpp PUBLIC ${FLUSS_CPP_ARROW_EXTRA_INCLUDE_DIRS})
endif()
target_link_libraries(fluss_cpp PUBLIC ${RUST_LIB})
target_link_libraries(fluss_cpp PRIVATE ${CMAKE_DL_LIBS} Threads::Threads)
target_link_libraries(fluss_cpp PUBLIC Arrow::arrow_shared)
target_compile_definitions(fluss_cpp PRIVATE ARROW_FOUND)
if(APPLE)
    target_link_libraries(fluss_cpp PUBLIC "-framework CoreFoundation" "-framework Security")
endif()

add_executable(fluss_cpp_example examples/example.cpp)
target_link_libraries(fluss_cpp_example PRIVATE fluss_cpp)
target_link_libraries(fluss_cpp_example PRIVATE Arrow::arrow_shared)
target_compile_definitions(fluss_cpp_example PRIVATE ARROW_FOUND)
target_include_directories(fluss_cpp_example PUBLIC ${CPP_INCLUDE_DIR})

add_executable(fluss_cpp_admin_example examples/admin_example.cpp)
target_link_libraries(fluss_cpp_admin_example PRIVATE fluss_cpp)
target_link_libraries(fluss_cpp_admin_example PRIVATE Arrow::arrow_shared)
target_compile_definitions(fluss_cpp_admin_example PRIVATE ARROW_FOUND)
target_include_directories(fluss_cpp_admin_example PUBLIC ${CPP_INCLUDE_DIR})

add_executable(fluss_cpp_kv_example examples/kv_example.cpp)
target_link_libraries(fluss_cpp_kv_example PRIVATE fluss_cpp)
target_link_libraries(fluss_cpp_kv_example PRIVATE Arrow::arrow_shared)
target_compile_definitions(fluss_cpp_kv_example PRIVATE ARROW_FOUND)
target_include_directories(fluss_cpp_kv_example PUBLIC ${CPP_INCLUDE_DIR})

if (CARGO_TARGET_DIR)
    set_target_properties(fluss_cpp
        PROPERTIES ADDITIONAL_CLEAN_FILES "${CARGO_TARGET_DIR}"
    )
endif()
add_dependencies(fluss_cpp cargo_build)

if (FLUSS_ENABLE_ADDRESS_SANITIZER)
    target_compile_options(fluss_cpp PRIVATE -fsanitize=leak,address,undefined -fno-omit-frame-pointer -fno-common -O1)
    target_link_options(fluss_cpp PRIVATE -fsanitize=leak,address,undefined)
endif()

if (FLUSS_ENABLE_TESTING)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v${FLUSS_GOOGLETEST_VERSION}.tar.gz
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googletest)

    if (NOT TARGET nlohmann_json::nlohmann_json)
        set(JSON_BuildTests OFF CACHE INTERNAL "")
        FetchContent_Declare(
            nlohmann_json
            URL https://github.com/nlohmann/json/archive/refs/tags/v${FLUSS_NLOHMANN_JSON_VERSION}.tar.gz
            URL_HASH SHA256=4b92eb0c06d10683f7447ce9406cb97cd4b453be18d7279320f7b2f025c10187
        )
        FetchContent_MakeAvailable(nlohmann_json)
    endif()

    enable_testing()
    include(GoogleTest)

    file(GLOB TEST_SOURCE_FILES "test/*.cpp")
    add_executable(fluss_cpp_test ${TEST_SOURCE_FILES})
    target_link_libraries(fluss_cpp_test PRIVATE fluss_cpp GTest::gtest nlohmann_json::nlohmann_json)
    target_link_libraries(fluss_cpp_test PRIVATE Arrow::arrow_shared)
    target_compile_definitions(fluss_cpp_test PRIVATE ARROW_FOUND)
    target_include_directories(fluss_cpp_test PRIVATE
        ${CPP_INCLUDE_DIR}
        ${PROJECT_SOURCE_DIR}/test
    )

    gtest_discover_tests(fluss_cpp_test
        PROPERTIES
            TIMEOUT 120
            FIXTURES_REQUIRED fluss_cluster
    )

    add_test(NAME fluss_cluster_cleanup COMMAND fluss_cpp_test --cleanup)
    set_tests_properties(fluss_cluster_cleanup PROPERTIES
        FIXTURES_CLEANUP fluss_cluster
    )
endif()
