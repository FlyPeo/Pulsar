cmake_minimum_required(VERSION 3.16)
project(Pulsar VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(GNUInstallDirs)
find_package(Threads REQUIRED)

option(PULSAR_BUILD_TESTS "Build Pulsar correctness tests" ON)
option(PULSAR_BUILD_BENCHMARKS "Build Pulsar benchmarks" ON)

add_library(pulsar STATIC
    src/fd_manager.cpp
    src/fiber.cpp
    src/hook.cpp
    src/iomanager.cpp
    src/scheduler.cpp
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
target_link_libraries(pulsar PUBLIC Threads::Threads ${CMAKE_DL_LIBS})

if(BUILD_TESTING AND PULSAR_BUILD_TESTS)
    add_executable(pulsar-sync-check tests/fiber_sync_check.cpp)
    target_link_libraries(pulsar-sync-check PRIVATE Pulsar::pulsar)
    add_test(NAME pulsar-sync-check COMMAND pulsar-sync-check)
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
