-- Time Between Engine Test And Actual Launch
-- Show average and max delay (in hours) between first engine fire time (static_fire_date_utc) and actual launch times (date_utc), grouped by year.
SELECT
    EXTRACT(
        YEAR
        FROM
            date_utc
    ) AS launch_year,
    COUNT(*) AS launches_with_static_fire,
    ROUND(
        AVG(
            date_diff ('hour', static_fire_date_utc, date_utc)
        ),
        2
    ) AS avg_delay_hours,
    ROUND(
        MAX(
            date_diff ('hour', static_fire_date_utc, date_utc)
        ),
        2
    ) AS max_delay_hours
FROM
    postgresql.public.raw_launches
WHERE
    static_fire_date_utc IS NOT NULL
    AND date_utc IS NOT NULL
    AND static_fire_date_utc <= date_utc -- Ensure static fire happened before launch
GROUP BY
    EXTRACT(
        YEAR
        FROM
            date_utc
    )
ORDER BY
    launch_year;

--  launch_year | launches_with_static_fire | avg_delay_hours | max_delay_hours 
-- -------------+---------------------------+-----------------+-----------------
--         2006 |                         1 |           190.0 |             190 
--         2008 |                         1 |           215.0 |             215 
--         2010 |                         2 |          1060.5 |            2010 
--         2012 |                         2 |           375.5 |             535 
--         2013 |                         3 |          210.67 |             280 
--         2014 |                         5 |           361.4 |            1003 
--         2015 |                         7 |           183.0 |             537 
--         2016 |                         8 |           71.88 |             141 
--         2017 |                        18 |          137.17 |             218 
--         2018 |                        21 |          229.76 |            1370 
--         2019 |                        13 |          189.62 |             876 
--         2020 |                        21 |          222.05 |            1042 
--         2021 |                        14 |           121.0 |             332 
--         2022 |                         3 |          132.67 |             193