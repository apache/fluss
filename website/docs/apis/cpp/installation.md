---
sidebar_position: 1
---
# Installation

The C++ bindings are not yet published as a package. You need to build from source.

**Prerequisites:** CMake 3.22+, C++17 compiler, Rust 1.85+, Apache Arrow C++ library

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust
```

Install dependencies:

```bash
# macOS
brew install cmake arrow

# Ubuntu/Debian
sudo apt-get install cmake libarrow-dev
```

If Arrow is not available via package manager, build from source:

```bash
git clone https://github.com/apache/arrow.git
cd arrow/cpp
cmake -B build -DARROW_BUILD_SHARED=ON
cmake --build build
sudo cmake --install build
```

Build the C++ bindings:

```bash
cd bindings/cpp
mkdir -p build && cd build

# Debug mode
cmake ..

# Or Release mode
cmake -DCMAKE_BUILD_TYPE=Release ..

# Build
cmake --build .
```

This produces:
- `libfluss_cpp.a` (Static library)
- `fluss_cpp_example` (Example executable)
- Header files in `include/`

## Integrating into Your Project

**Option 1: CMake FetchContent**

```cmake
include(FetchContent)
FetchContent_Declare(
    fluss-cpp
    GIT_REPOSITORY https://github.com/apache/fluss-rust.git
    SOURCE_SUBDIR bindings/cpp
)
FetchContent_MakeAvailable(fluss-cpp)

target_link_libraries(your_target PRIVATE fluss_cpp)
```

**Option 2: Manual Integration**

Copy the build artifacts and configure CMake:

```cmake
find_package(Arrow REQUIRED)

add_library(fluss_cpp STATIC IMPORTED)
set_target_properties(fluss_cpp PROPERTIES
    IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/lib/libfluss_cpp.a
    INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/include
)

target_link_libraries(your_target
    PRIVATE
    fluss_cpp
    Arrow::arrow_shared
    ${CMAKE_DL_LIBS}
    Threads::Threads
)

# On macOS, also link these frameworks
if(APPLE)
    target_link_libraries(your_target PRIVATE
        "-framework CoreFoundation"
        "-framework Security"
    )
endif()
```

**Option 3: Subdirectory**

```cmake
add_subdirectory(vendor/fluss-rust/bindings/cpp)
target_link_libraries(your_target PRIVATE fluss_cpp)
```
