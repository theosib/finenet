cmake_minimum_required(VERSION 3.16)

if(APPLE AND (NOT CMAKE_OSX_SYSROOT OR NOT EXISTS "${CMAKE_OSX_SYSROOT}"))
    set(_xcode_sdk "/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk")
    if(EXISTS "${_xcode_sdk}")
        set(CMAKE_OSX_SYSROOT "${_xcode_sdk}" CACHE PATH "macOS SDK path" FORCE)
    endif()
endif()

project(finenet VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ── Library ──────────────────────────────────────────────────────────────────

add_library(finenet
    src/message.cpp
    src/connection.cpp
    src/endpoint.cpp
    src/stream_transport.cpp
    src/listener.cpp
    src/discovery.cpp
)
add_library(finenet::finenet ALIAS finenet)

target_include_directories(finenet PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(finenet PUBLIC cxx_std_20)

find_package(Threads REQUIRED)
target_link_libraries(finenet PUBLIC Threads::Threads)

# ── Tests ────────────────────────────────────────────────────────────────────

option(FINENET_BUILD_TESTS "Build finenet tests" ON)

if(FINENET_BUILD_TESTS)
    enable_testing()

    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
    )
    FetchContent_MakeAvailable(googletest)

    add_executable(finenet_tests
        tests/test_wake_signal.cpp
        tests/test_queue.cpp
        tests/test_keyed_queue.cpp
        tests/test_message.cpp
        tests/test_connection.cpp
        tests/test_queue_bridge.cpp
        tests/test_endpoint.cpp
        tests/test_stream_transport.cpp
        tests/test_discovery.cpp
    )

    target_link_libraries(finenet_tests PRIVATE finenet GTest::gtest GTest::gtest_main)
    target_compile_features(finenet_tests PRIVATE cxx_std_20)

    include(GoogleTest)
    gtest_discover_tests(finenet_tests)
endif()
