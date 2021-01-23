SELECT
    name,
    COUNT(*) AS name_count
FROM
    bigquery-public-data.usa_names.usa_1910_2012
WHERE
    state = 'WA'
GROUP BY
    name