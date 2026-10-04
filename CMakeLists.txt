cmake_minimum_required(VERSION 3.16)
project(Pulsar VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)
find_package(Boost 1.61 REQUIRED COMPONENTS context)

option(PULSAR_BUILD_TESTS "Build Pulsar correctness tests" ON)
option(PULSAR_BUILD_BENCHMARKS "Build Pulsar benchmarks" ON)
option(PULSAR_BUILD_NET "Build the optional explicit TCP network module" OFF)
option(PULSAR_BUILD_RPC "Build the optional Protobuf RPC module" OFF)
set(PULSAR_LEGACY_INTEROP_BINARY "" CACHE FILEPATH
    "Optional separately linked StrataKV RPC endpoint for cross-process tests")
option(PULSAR_FIBER_GUARD_PAGES "Protect Fiber stack overflow with an mmap guard page" OFF)

if(PULSAR_BUILD_RPC)
    set(PULSAR_BUILD_NET ON)
endif()

add_library(pulsar STATIC
    src/fd_manager.cpp
    src/fiber.cpp
    src/hook.cpp
    src/iomanager.cpp
    src/scheduler.cpp
    src/stack_allocator.cpp
    src/sync.cpp
    src/thread.cpp
    src/timer.cpp
    src/utils.cpp
)
add_library(Pulsar::pulsar ALIAS pulsar)

target_compile_features(pulsar PUBLIC cxx_std_17)
target_include_directories(pulsar
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include/pulsar
)
target_link_libraries(pulsar PUBLIC Threads::Threads ${CMAKE_DL_LIBS} Boost::context)
target_compile_definitions(pulsar PUBLIC PULSAR_CONTEXT_BACKEND_BOOST_FCONTEXT=1)
if(PULSAR_FIBER_GUARD_PAGES)
    target_compile_definitions(pulsar PRIVATE PULSAR_FIBER_GUARD_PAGES=1)
endif()

if(PULSAR_BUILD_NET)
    add_library(pulsar_net STATIC src/net.cpp)
    add_library(Pulsar::net ALIAS pulsar_net)
    target_compile_features(pulsar_net PUBLIC cxx_std_17)
    target_include_directories(pulsar_net PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(pulsar_net PUBLIC Pulsar::pulsar)
    set_target_properties(pulsar_net PROPERTIES EXPORT_NAME net)
    install(TARGETS pulsar_net EXPORT PulsarTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
    add_executable(pulsar-tcp-echo examples/tcp_echo.cpp)
    target_link_libraries(pulsar-tcp-echo PRIVATE Pulsar::net)
endif()

if(PULSAR_BUILD_RPC)
    find_package(Protobuf REQUIRED)
    protobuf_generate_cpp(PULSAR_RPC_PROTO_SOURCES PULSAR_RPC_PROTO_HEADERS
        proto/pulsar_rpc_header.proto)
    add_library(pulsar_rpc STATIC
        src/rpc.cpp
        ${PULSAR_RPC_PROTO_SOURCES}
        ${PULSAR_RPC_PROTO_HEADERS}
    )
    add_library(Pulsar::rpc ALIAS pulsar_rpc)
    target_compile_features(pulsar_rpc PUBLIC cxx_std_17)
    target_include_directories(pulsar_rpc PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_include_directories(pulsar_rpc PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries(pulsar_rpc PUBLIC Pulsar::net protobuf::libprotobuf)
    set_target_properties(pulsar_rpc PROPERTIES EXPORT_NAME rpc)
    install(TARGETS pulsar_rpc EXPORT PulsarTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})

    protobuf_generate_cpp(PULSAR_ECHO_PROTO_SOURCES PULSAR_ECHO_PROTO_HEADERS
        examples/rpc_echo.proto)
    add_executable(pulsar-rpc-echo
        examples/rpc_echo.cpp
        ${PULSAR_ECHO_PROTO_SOURCES}
        ${PULSAR_ECHO_PROTO_HEADERS}
    )
    target_include_directories(pulsar-rpc-echo PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
    target_link_libraries(pulsar-rpc-echo PRIVATE Pulsar::rpc)
    if(BUILD_TESTING AND PULSAR_BUILD_TESTS)
        add_executable(pulsar-rpc-interop-new
            tests/interop/new_endpoint.cpp
            ${PULSAR_ECHO_PROTO_SOURCES}
            ${PULSAR_ECHO_PROTO_HEADERS}
        )
        target_include_directories(pulsar-rpc-interop-new PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
        target_link_libraries(pulsar-rpc-interop-new PRIVATE Pulsar::rpc)
        if(PULSAR_LEGACY_INTEROP_BINARY)
            find_package(Python3 COMPONENTS Interpreter REQUIRED)
            add_test(NAME pulsar-rpc-legacy-interop
                COMMAND Python3::Interpreter
                    ${CMAKE_CURRENT_SOURCE_DIR}/tests/interop/run.py
                    $<TARGET_FILE:pulsar-rpc-interop-new>
                    ${PULSAR_LEGACY_INTEROP_BINARY})
        endif()
    endif()
endif()

if(BUILD_TESTING AND PULSAR_BUILD_TESTS)
    target_compile_definitions(pulsar PRIVATE PULSAR_ENABLE_TEST_HOOKS=1)

    add_executable(pulsar-sync-check tests/fiber_sync_check.cpp)
    target_link_libraries(pulsar-sync-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-sync-check COMMAND pulsar-sync-check)

    add_executable(pulsar-fiber-context-check tests/fiber_context_check.cpp)
    target_link_libraries(pulsar-fiber-context-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-fiber-context-check COMMAND pulsar-fiber-context-check)

    add_executable(pulsar-scheduler-work-stealing-check tests/scheduler_work_stealing_check.cpp)
    target_link_libraries(pulsar-scheduler-work-stealing-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-scheduler-work-stealing-check COMMAND pulsar-scheduler-work-stealing-check)

    add_executable(pulsar-stack-pool-check tests/stack_pool_check.cpp)
    target_compile_definitions(pulsar-stack-pool-check PRIVATE PULSAR_ENABLE_TEST_HOOKS=1)
    target_link_libraries(pulsar-stack-pool-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-stack-pool-check COMMAND pulsar-stack-pool-check)

    add_executable(pulsar-scheduler-cache-check tests/scheduler_cache_check.cpp)
    target_link_libraries(pulsar-scheduler-cache-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-scheduler-cache-check COMMAND pulsar-scheduler-cache-check)

    add_executable(pulsar-reuse-integration-check tests/reuse_integration_check.cpp)
    target_link_libraries(pulsar-reuse-integration-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-reuse-integration-check COMMAND pulsar-reuse-integration-check)

    add_executable(pulsar-file-check tests/file_check.cpp)
    target_link_libraries(pulsar-file-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-file-check COMMAND pulsar-file-check)

    if(PULSAR_BUILD_NET)
        add_executable(pulsar-net-check tests/net_check.cpp)
        target_link_libraries(pulsar-net-check PRIVATE Pulsar::net)
        add_test(NAME pulsar-net-check COMMAND pulsar-net-check)
    endif()
    if(PULSAR_BUILD_RPC)
        add_test(NAME pulsar-rpc-echo COMMAND pulsar-rpc-echo)
        add_executable(pulsar-rpc-fault-check
            tests/rpc_fault_check.cpp
            ${PULSAR_ECHO_PROTO_SOURCES}
            ${PULSAR_ECHO_PROTO_HEADERS}
        )
        target_include_directories(pulsar-rpc-fault-check PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
        target_link_libraries(pulsar-rpc-fault-check PRIVATE Pulsar::rpc)
        add_test(NAME pulsar-rpc-fault-check COMMAND pulsar-rpc-fault-check)

        add_executable(pulsar-rpc-multiplex-check
            tests/rpc_multiplex_check.cpp
            ${PULSAR_ECHO_PROTO_SOURCES}
            ${PULSAR_ECHO_PROTO_HEADERS}
        )
        target_include_directories(pulsar-rpc-multiplex-check PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
        target_link_libraries(pulsar-rpc-multiplex-check PRIVATE Pulsar::rpc)
        add_test(NAME pulsar-rpc-multiplex-check COMMAND pulsar-rpc-multiplex-check)
    endif()
endif()

if(PULSAR_BUILD_BENCHMARKS)
    add_executable(pulsar-benchmark benchmarks/fiber_benchmark.cpp)
    target_link_libraries(pulsar-benchmark PRIVATE Pulsar::pulsar)
endif()

install(TARGETS pulsar
    EXPORT PulsarTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/pulsar DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT PulsarTargets
    FILE PulsarTargets.cmake
    NAMESPACE Pulsar::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Pulsar
)
configure_package_config_file(
    cmake/PulsarConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/PulsarConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Pulsar
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/PulsarConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/PulsarConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/PulsarConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Pulsar
)
