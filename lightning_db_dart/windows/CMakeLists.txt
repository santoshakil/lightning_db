cmake_minimum_required(VERSION 3.10)

set(PROJECT_NAME "lightning_db_dart")
project(${PROJECT_NAME} LANGUAGES CXX)

include(../cargokit/cmake/cargokit.cmake)

apply_cargokit(lightning_db_ffi ../../lightning_db_ffi ${CMAKE_CURRENT_BINARY_DIR})

add_subdirectory("${CMAKE_CURRENT_SOURCE_DIR}/../src" "${CMAKE_CURRENT_BINARY_DIR}/shared")

set(lightning_db_dart_bundled_libraries
  $<TARGET_FILE:lightning_db_ffi>
  PARENT_SCOPE
)