-- How many launches have occurred at each launch site, and what's the average payload per site?
SELECT
    launchpad_id AS launch_site,
    COUNT(*) AS total_launches,
    ROUND(AVG(total_payload_mass_kg), 2) AS average_payload_mass_kg
FROM
    raw_launches
WHERE
    launchpad_id IS NOT NULL
GROUP BY
    launchpad_id
ORDER BY
    total_launches DESC;

--        launch_site        | total_launches | average_payload_mass_kg 
-- --------------------------+----------------+-------------------------
--  5e9e4501f509094ba4566f84 |            112 |                 7547.89 
--  5e9e4502f509094188566f88 |             58 |                 9427.57 
--  5e9e4502f509092b78566f87 |             30 |                 7588.19 
--  5e9e4502f5090995de566f86 |              5 |                  128.33