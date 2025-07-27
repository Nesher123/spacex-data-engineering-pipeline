-- How has the success rate of launches evolved year over year?
SELECT
    EXTRACT(
        YEAR
        FROM
            updated_at
    ) AS year,
    ROUND(AVG(success_rate), 2) AS avg_success_rate
FROM
    launch_aggregations
WHERE
    success_rate IS NOT NULL
GROUP BY
    EXTRACT(
        YEAR
        FROM
            updated_at
    )
ORDER BY
    year;

--  year | avg_success_rate 
-- ------+------------------
--  2025 |            88.29