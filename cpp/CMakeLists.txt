cmake_minimum_required(VERSION 3.14)
project(basis_rs_cpp_tests LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Find the Rust static library
set(RUST_LIB_DIR "${CMAKE_SOURCE_DIR}/../target/release")
set(RUST_LIB_DEBUG_DIR "${CMAKE_SOURCE_DIR}/../target/debug")

# Try release first, then debug
if(EXISTS "${RUST_LIB_DIR}/libbasis_rs.a")
    set(BASIS_RS_LIB "${RUST_LIB_DIR}/libbasis_rs.a")
    set(RUST_BUILD_TYPE "release")
    message(STATUS "Using release Rust library: ${BASIS_RS_LIB}")
elseif(EXISTS "${RUST_LIB_DEBUG_DIR}/libbasis_rs.a")
    set(BASIS_RS_LIB "${RUST_LIB_DEBUG_DIR}/libbasis_rs.a")
    set(RUST_BUILD_TYPE "debug")
    message(STATUS "Using debug Rust library: ${BASIS_RS_LIB}")
else()
    message(FATAL_ERROR "Rust library not found. Run 'cargo build --release' first.")
endif()

# CXX bridge directories
set(CXX_BRIDGE_DIR "${CMAKE_SOURCE_DIR}/../target/cxxbridge")
set(CXX_BRIDGE_SRC "${CXX_BRIDGE_DIR}/basis-rs/src/cxx_bridge.rs.cc")

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/../include
    ${CXX_BRIDGE_DIR}
    ${CXX_BRIDGE_DIR}/basis-rs/src
    ${CXX_BRIDGE_DIR}/rust
)

# Fetch GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.14.0
)
# Prevent GoogleTest from overriding our compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# System libraries required by Rust
if(UNIX AND NOT APPLE)
    set(SYSTEM_LIBS pthread dl m)
elseif(APPLE)
    set(SYSTEM_LIBS pthread dl)
    find_library(SECURITY_FRAMEWORK Security)
    find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
    list(APPEND SYSTEM_LIBS ${SECURITY_FRAMEWORK} ${COREFOUNDATION_FRAMEWORK})
endif()

# Legacy FFI test executable
add_executable(parquet_test
    tests/parquet_test.cpp
)

target_link_libraries(parquet_test
    ${BASIS_RS_LIB}
    GTest::gtest
    GTest::gtest_main
    ${SYSTEM_LIBS}
)

# CXX-based ParquetCodec test executable
add_executable(parquet_codec_test
    tests/parquet_codec_test.cpp
    ${CXX_BRIDGE_SRC}
)

target_link_libraries(parquet_codec_test
    ${BASIS_RS_LIB}
    GTest::gtest
    GTest::gtest_main
    ${SYSTEM_LIBS}
)

# Enable testing
enable_testing()
add_test(NAME ParquetTest COMMAND parquet_test)
add_test(NAME ParquetCodecTest COMMAND parquet_codec_test)
