# The Flutter tooling requires that developers have a version of Visual Studio
# installed that includes CMake 3.14 or later. You should not increase this
# version, as doing so will cause the plugin to fail to compile for some
# customers of the plugin.
cmake_minimum_required(VERSION 3.14)

# Project-level configuration.
set(PROJECT_NAME "lightning_db")
project(${PROJECT_NAME} LANGUAGES CXX)

# Invoke the build for native code shared with the other target platforms.
# This can be changed to accommodate different builds.
add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../src" "${CMAKE_CURRENT_BINARY_DIR}/shared")

# List of absolute paths to libraries that should be bundled with the plugin.
# This list could contain prebuilt libraries, or libraries created by an
# external build triggered from this build file.
set(lightning_db_bundled_libraries
  "${CMAKE_CURRENT_SOURCE_DIR}/lightning_db.dll"
  PARENT_SCOPE
)

# Download the pre-built library
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/lightning_db.dll")
  execute_process(
    COMMAND dart run lightning_db_dart:install --target-os-type windows
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../.."
    RESULT_VARIABLE DOWNLOAD_RESULT
  )
  if(DOWNLOAD_RESULT)
    message(FATAL_ERROR "Failed to download Lightning DB library")
  endif()
  
  # Copy the library to the expected location
  file(COPY "${CMAKE_CURRENT_SOURCE_DIR}/../../packages/lightning_db_dart/windows/lightning_db.dll"
       DESTINATION "${CMAKE_CURRENT_SOURCE_DIR}/")
endif()