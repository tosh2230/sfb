SELECT
    *
FROM
    bigquery-public-data.covid19_open_data.covid19_open_data
WHERE
    date >= @ds_start_date
    AND date <= @ds_end_date
