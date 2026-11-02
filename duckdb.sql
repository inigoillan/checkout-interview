INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET localstack_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID 'test',
    SECRET 'test',
    REGION 'eu-west-1',
    ENDPOINT 'http://localhost:4566',
    USE_SSL FALSE,
    URL_STYLE 'path'
);

CREATE OR REPLACE TABLE raw_views AS SELECT * EXCEPT(timestamp), make_timestamp_ms(timestamp*1000) AS timestamp FROM read_parquet('s3://checkout-lake-raw/landing/**/');
CREATE OR REPLACE TABLE counts AS SELECT * FROM read_parquet('s3://checkout-lake-raw/counts/**');


