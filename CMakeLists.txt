cmake_minimum_required(VERSION 3.17)
project(basis-rs VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
include(BuildHelpers)

option(BASIS_RS_BUILD_TESTS "Build unit tests" OFF)

# ============================================================
# Locate Rust build artifacts
# ============================================================

set(RUST_TARGET_DIR "${CMAKE_CURRENT_SOURCE_DIR}/target")
set(RUST_TARGET_TRIPLE "" CACHE STRING "Rust target triple (e.g. x86_64-unknown-linux-gnu). If set, look in target/<triple>/{release,debug}/.")

if(RUST_TARGET_TRIPLE)
    set(_RUST_RELEASE_DIR "${RUST_TARGET_DIR}/${RUST_TARGET_TRIPLE}/release")
    set(_RUST_DEBUG_DIR   "${RUST_TARGET_DIR}/${RUST_TARGET_TRIPLE}/debug")
else()
    set(_RUST_RELEASE_DIR "${RUST_TARGET_DIR}/release")
    set(_RUST_DEBUG_DIR   "${RUST_TARGET_DIR}/debug")
endif()

# Rust static library (try release first, then debug)
set(RUST_LIB_RELEASE "${_RUST_RELEASE_DIR}/libbasis_rs.a")
set(RUST_LIB_DEBUG "${_RUST_DEBUG_DIR}/libbasis_rs.a")

if(EXISTS "${RUST_LIB_RELEASE}")
    set(RUST_LIB "${RUST_LIB_RELEASE}")
    message(STATUS "Found Rust library (release): ${RUST_LIB}")
elseif(EXISTS "${RUST_LIB_DEBUG}")
    set(RUST_LIB "${RUST_LIB_DEBUG}")
    message(STATUS "Found Rust library (debug): ${RUST_LIB}")
else()
    message(FATAL_ERROR
        "Rust library not found. Run 'cargo build --release' first.\n"
        "Looked in:\n  ${RUST_LIB_RELEASE}\n  ${RUST_LIB_DEBUG}")
endif()

# CXX bridge generated files
# cxx-build may place cxxbridge output at target/cxxbridge/ or
# target/<triple>/cxxbridge/ depending on whether --target was used.
if(RUST_TARGET_TRIPLE AND EXISTS "${RUST_TARGET_DIR}/${RUST_TARGET_TRIPLE}/cxxbridge")
    set(CXX_BRIDGE_DIR "${RUST_TARGET_DIR}/${RUST_TARGET_TRIPLE}/cxxbridge")
else()
    set(CXX_BRIDGE_DIR "${RUST_TARGET_DIR}/cxxbridge")
endif()
set(CXX_BRIDGE_CC "${CXX_BRIDGE_DIR}/basis-rs/src/cxx_bridge.rs.cc")
set(CXX_BRIDGE_H "${CXX_BRIDGE_DIR}/basis-rs/src/cxx_bridge.rs.h")

if(NOT EXISTS "${CXX_BRIDGE_CC}")
    message(FATAL_ERROR
        "CXX bridge source not found: ${CXX_BRIDGE_CC}\n"
        "Run 'cargo build --release' first.")
endif()

# ============================================================
# External dependencies (for absl time types)
# ============================================================

find_package(absl REQUIRED)

# ============================================================
# basis_rs_parquet library
# ============================================================

add_library(basis_rs_parquet STATIC "${CXX_BRIDGE_CC}")
target_compile_features(basis_rs_parquet PUBLIC cxx_std_17)

# Include directories:
#   Build tree: include/ for <basis_rs/parquet.hpp>, cxxbridge dir for "cxx_bridge.rs.h"
#   Install tree: single include dir has both
target_include_directories(basis_rs_parquet
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CXX_BRIDGE_DIR}/basis-rs/src>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# Rust static library (imported)
add_library(basis_rs_rust STATIC IMPORTED)
set_target_properties(basis_rs_rust PROPERTIES
    IMPORTED_LOCATION "${RUST_LIB}"
)

# System libraries required by Rust runtime
if(UNIX AND NOT APPLE)
    set(RUST_SYSTEM_LIBS pthread dl m)
elseif(APPLE)
    set(RUST_SYSTEM_LIBS pthread dl)
    find_library(SECURITY_FRAMEWORK Security)
    find_library(COREFOUNDATION_FRAMEWORK CoreFoundation)
    list(APPEND RUST_SYSTEM_LIBS ${SECURITY_FRAMEWORK} ${COREFOUNDATION_FRAMEWORK})
endif()

target_link_libraries(basis_rs_parquet
    PUBLIC basis_rs_rust ${RUST_SYSTEM_LIBS}
    PUBLIC absl::time
)

add_library(basis_rs::parquet ALIAS basis_rs_parquet)

# ============================================================
# Install
# ============================================================

# C++ bridge library
install(TARGETS basis_rs_parquet
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# Public header
install(DIRECTORY include/basis_rs
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# CXX bridge generated header (implementation detail needed by basis_rs/parquet.hpp)
# Resolve symlink — CXX bridge output may be a symlink into the Rust build tree
get_filename_component(CXX_BRIDGE_H_REAL "${CXX_BRIDGE_H}" REALPATH)
install(FILES "${CXX_BRIDGE_H_REAL}"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    RENAME cxx_bridge.rs.h
)

# Rust static library
install(FILES "${RUST_LIB}"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RENAME libbasis_rs.a
)

# CMake package config
configure_package_config_file(
    cmake/Config.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/basis-rsConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/basis-rs"
)

write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/basis-rsConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/basis-rsConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/basis-rsConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/basis-rs"
)

# ============================================================
# Tests
# ============================================================

if(BASIS_RS_BUILD_TESTS)
    enable_testing()
    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        include(FetchContent)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.14.0
        )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(googletest)
    endif()
    include(GoogleTest)
    add_subdirectory(cpp/tests)
endif()
