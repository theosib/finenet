cmake_minimum_required(VERSION 3.16)
project(finenet VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Library ──────────────────────────────────────────────────────────────────

add_library(finenet
    src/placeholder.cpp   # Placeholder until real sources are added
)

target_include_directories(finenet PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

target_compile_features(finenet PUBLIC cxx_std_17)

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
    )

    target_link_libraries(finenet_tests PRIVATE finenet GTest::gtest GTest::gtest_main)
    target_compile_features(finenet_tests PRIVATE cxx_std_17)

    include(GoogleTest)
    gtest_discover_tests(finenet_tests)
endif()
