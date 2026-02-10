include(CMakeParseArguments)

function(make_cc_test)
    if(NOT BASIS_RS_BUILD_TESTS)
        return()
    endif()

    cmake_parse_arguments(MAKE_CC_TEST
        ""
        "NAME"
        "SRCS;DEPS;DATA"
        ${ARGN})

    set(_NAME "${MAKE_CC_TEST_NAME}")

    add_executable(${_NAME})
    target_sources(${_NAME} PRIVATE ${MAKE_CC_TEST_SRCS})
    target_link_libraries(${_NAME}
        PRIVATE ${MAKE_CC_TEST_DEPS} GTest::gtest GTest::gtest_main)
    gtest_discover_tests(${_NAME})
endfunction()
