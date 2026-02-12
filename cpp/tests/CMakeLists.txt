make_cc_test(
    NAME parquet_codec_test
    SRCS parquet_codec_test.cpp
    DEPS basis_rs::parquet
)

# Benchmark executable (not a test, just a tool)
add_executable(parquet_benchmark parquet_benchmark.cpp)
target_link_libraries(parquet_benchmark PRIVATE basis_rs::parquet)
