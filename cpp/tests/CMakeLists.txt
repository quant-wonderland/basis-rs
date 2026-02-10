make_cc_test(
    NAME parquet_codec_test
    SRCS parquet_codec_test.cpp
    DEPS basis_rs::parquet
)
