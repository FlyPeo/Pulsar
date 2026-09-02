cmake_minimum_required(VERSION 3.16)
project(Pulsar VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads REQUIRED)
find_package(Boost 1.61 REQUIRED COMPONENTS context)

option(PULSAR_BUILD_TESTS "Build Pulsar correctness tests" ON)
option(PULSAR_BUILD_BENCHMARKS "Build Pulsar benchmarks" ON)
option(PULSAR_FIBER_GUARD_PAGES "Protect Fiber stack overflow with an mmap guard page" OFF)

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
