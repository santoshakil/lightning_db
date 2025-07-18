# The Flutter tooling requires that developers have CMake 3.10 or later
# installed. You should not increase this version, as doing so will cause
# the plugin to fail to compile for some customers of the plugin.
cmake_minimum_required(VERSION 3.10)

# Project-level configuration.
set(PROJECT_NAME "lightning_db")
project(${PROJECT_NAME} LANGUAGES CXX)

# This value is used when generating builds using this plugin, so it must
# not be changed.
set(PLUGIN_NAME "lightning_db_plugin")

# Define the plugin library target. Its name must not be changed (see comment
# on PLUGIN_NAME above).
#
# Any new source files that you add to the plugin should be added here.
add_library(${PLUGIN_NAME} SHARED
  "lightning_db_plugin.cc"
)

# Apply a standard set of build settings that are configured in the
# application-level CMakeLists.txt. This can be removed for plugins that want
# full control over build settings.
apply_standard_settings(${PLUGIN_NAME})

# Symbols are hidden by default to reduce the chance of accidental conflicts
# between plugins. This should not be removed; any symbols that should be
# exported should be explicitly exported with the FLUTTER_PLUGIN_EXPORT macro.
set_target_properties(${PLUGIN_NAME} PROPERTIES
  CXX_VISIBILITY_PRESET hidden)
target_compile_definitions(${PLUGIN_NAME} PRIVATE FLUTTER_PLUGIN_IMPL)

# Source include directories and library dependencies. Add any plugin-specific
# dependencies here.
target_include_directories(${PLUGIN_NAME} INTERFACE
  "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(${PLUGIN_NAME} PRIVATE flutter)
target_link_libraries(${PLUGIN_NAME} PRIVATE PkgConfig::GTK)

# List of absolute paths to libraries that should be bundled with the plugin.
# This list could contain prebuilt libraries, or libraries created by an
# external build triggered from this build file.
set(lightning_db_bundled_libraries
  "${CMAKE_CURRENT_SOURCE_DIR}/liblightning_db.so"
  PARENT_SCOPE
)

# Download the pre-built library
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/liblightning_db.so")
  execute_process(
    COMMAND dart run lightning_db_dart:install --target-os-type linux
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../.."
    RESULT_VARIABLE DOWNLOAD_RESULT
  )
  if(DOWNLOAD_RESULT)
    message(FATAL_ERROR "Failed to download Lightning DB library")
  endif()
  
  # Copy the library to the expected location
  file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/../../packages/lightning_db_dart/linux/liblightning_db.so"
       DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/")
endif()