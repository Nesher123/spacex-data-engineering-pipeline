-- List the top 5 launches with the heaviest total payload mass.
SELECT
    launch_id,
    mission_name,
    date_utc,
    total_payload_mass_kg,
    success,
    launchpad_id
FROM
    raw_launches
WHERE
    total_payload_mass_kg IS NOT NULL
ORDER BY
    total_payload_mass_kg DESC
LIMIT
    5;

--             launch_id         |              mission_name               |            date_utc            | total_payload_mass_kg>
-- --------------------------+-----------------------------------------+--------------------------------+---------------------->
--  5ed9819a1f30554030d45c29 | Starlink-9 (v1.0) & BlackSky Global 5-6 | 2020-08-07 05:12:00.000000 UTC |              15712.00>
--  5eb87d3cffd86e000604b380 | Starlink-2                              | 2020-01-07 02:19:00.000000 UTC |              15600.00>
--  5eb87d39ffd86e000604b37d | Starlink-1                              | 2019-11-11 14:56:00.000000 UTC |              15600.00>
--  5eb87d3fffd86e000604b382 | Starlink-3                              | 2020-01-29 14:06:00.000000 UTC |              15600.00>
--  5eb87d41ffd86e000604b383 | Starlink-4                              | 2020-02-17 15:05:55.000000 UTC |              15600.00>