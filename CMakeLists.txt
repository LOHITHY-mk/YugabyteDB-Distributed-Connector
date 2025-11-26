cmake_minimum_required(VERSION 3.15)
project(YBDistributedConnector VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Ensure PostgreSQL imported target is available (required by Poco::DataPostgreSQL)
find_package(PostgreSQL REQUIRED)

# Find required Poco components
find_package(Poco REQUIRED COMPONENTS Data DataPostgreSQL Net JSON)

# Include public headers
include_directories(${PROJECT_SOURCE_DIR}/include)

file(GLOB LIB_SOURCES
    src/*.cpp
)

add_library(YBDistributedConnector STATIC ${LIB_SOURCES})
target_include_directories(YBDistributedConnector PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(YBDistributedConnector PUBLIC Poco::Data Poco::DataPostgreSQL Poco::Net Poco::JSON)

add_executable(yb_connector_example main.cpp)
target_link_libraries(yb_connector_example PRIVATE YBDistributedConnector)

if(MSVC)
  target_compile_options(YBDistributedConnector PRIVATE /W4)
  target_compile_options(yb_connector_example PRIVATE /W4)
else()
  target_compile_options(YBDistributedConnector PRIVATE -Wall -Wextra -Wpedantic)
  target_compile_options(yb_connector_example PRIVATE -Wall -Wextra -Wpedantic)
endif()

# Tests
add_executable(connection_test tests/connection_test.cpp)
target_link_libraries(connection_test PRIVATE YBDistributedConnector)
set_target_properties(connection_test PROPERTIES RUNTIME_OUTPUT_NAME connection_test_run)

# Day 3: query_test
add_executable(query_test tests/query_test.cpp)
target_link_libraries(query_test PRIVATE YBDistributedConnector)

# Day 4: storage_test
add_executable(storage_test tests/storage_test.cpp)
target_link_libraries(storage_test PRIVATE YBDistributedConnector)
set_target_properties(storage_test PROPERTIES RUNTIME_OUTPUT_NAME storage_test_run)
set_target_properties(storage_test PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/bin
  RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/tests/bin/Release
  RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/tests/bin/Debug
  RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/tests/bin/RelWithDebInfo
  RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/tests/bin/MinSizeRel
)

# Day 5: integration_test
add_executable(integration_test tests/integration_test.cpp)
target_link_libraries(integration_test PRIVATE YBDistributedConnector)
set_target_properties(integration_test PROPERTIES RUNTIME_OUTPUT_NAME integration_test_run)
set_target_properties(integration_test PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests/bin
  RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_BINARY_DIR}/tests/bin/Release
  RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_BINARY_DIR}/tests/bin/Debug
  RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_BINARY_DIR}/tests/bin/RelWithDebInfo
  RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_BINARY_DIR}/tests/bin/MinSizeRel
)


