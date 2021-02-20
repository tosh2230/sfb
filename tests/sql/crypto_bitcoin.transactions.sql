SELECT
    *
FROM
    bigquery-public-data.crypto_bitcoin.transactions
WHERE
    block_timestamp_month >= @ds_start_date
    AND block_timestamp_month <= @ds_end_date
