cmake_minimum_required(VERSION 3.20)
project(IceLog CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(GTest REQUIRED)
find_package(benchmark REQUIRED)

# Proto codegen
get_target_property(gRPC_CPP_PLUGIN_EXECUTABLE gRPC::grpc_cpp_plugin LOCATION)

set(PROTO_FILE "${CMAKE_SOURCE_DIR}/proto/metadata_service.proto")
set(PROTO_OUT "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_OUT})

add_custom_command(
    OUTPUT "${PROTO_OUT}/metadata_service.pb.cc" "${PROTO_OUT}/metadata_service.pb.h"
           "${PROTO_OUT}/metadata_service.grpc.pb.cc" "${PROTO_OUT}/metadata_service.grpc.pb.h"
    COMMAND protobuf::protoc
    ARGS --grpc_out="${PROTO_OUT}" --cpp_out="${PROTO_OUT}"
         --plugin=protoc-gen-grpc="${gRPC_CPP_PLUGIN_EXECUTABLE}"
         -I "${CMAKE_SOURCE_DIR}/proto" "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
)

# Main server binary
add_executable(metadata_server
    src/main.cpp
    src/server/metadata_server.cpp
    src/catalog/catalog_manager.cpp
    src/catalog/partition_registry.cpp
    src/catalog/schema_store.cpp
    src/transaction/mvcc_manager.cpp
    src/transaction/snapshot.cpp
    src/lock/lock_manager.cpp
    src/db/pg_client.cpp
    src/db/connection_pool.cpp
    "${PROTO_OUT}/metadata_service.pb.cc"
    "${PROTO_OUT}/metadata_service.grpc.pb.cc"
)

target_include_directories(metadata_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_OUT}
)

target_link_libraries(metadata_server
    gRPC::grpc++
    protobuf::libprotobuf
    PostgreSQL::PostgreSQL
    pthread
)

# Unit tests
enable_testing()
add_executable(unit_tests
    tests/catalog_test.cpp
    tests/mvcc_test.cpp
    tests/lock_manager_test.cpp
    tests/lru_cache_test.cpp
    tests/connection_pool_test.cpp
)

target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${PROTO_OUT}
)

target_link_libraries(unit_tests
    GTest::gtest_main
    PostgreSQL::PostgreSQL
    pthread
)

add_test(NAME UnitTests COMMAND unit_tests)

# Benchmarks
add_executable(partition_bench benchmarks/partition_lookup_bench.cpp)

target_include_directories(partition_bench PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(partition_bench benchmark::benchmark)
