cmake_minimum_required(VERSION 3.16)
project(pulsar_legacy_interop LANGUAGES CXX)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
if(NOT STRATA_ROOT)
    message(FATAL_ERROR "Pass -DSTRATA_ROOT=/absolute/path/to/StrataKV")
endif()
protobuf_generate_cpp(ECHO_SOURCES ECHO_HEADERS rpc_echo.proto)
add_executable(pulsar-rpc-interop-legacy legacy_endpoint.cpp ${ECHO_SOURCES} ${ECHO_HEADERS})
target_compile_features(pulsar-rpc-interop-legacy PRIVATE cxx_std_17)
target_include_directories(pulsar-rpc-interop-legacy PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${STRATA_ROOT}/src/rpc/include
    ${STRATA_ROOT}/src/common/include
    ${STRATA_ROOT}/build/stratakv/release/src/rpc
)
target_link_libraries(pulsar-rpc-interop-legacy PRIVATE
    ${STRATA_ROOT}/lib/libstratakv_rpc.a
    boost_serialization muduo_net muduo_base Threads::Threads dl protobuf::libprotobuf
)
