USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_gender_popularity
    COMMENT "Serving view for event popularity by gender" AS
SELECT
    e.event_unit_type,
    e.event_name,

    a.athlete_gender,
    a.athlete_country_code,
    c.country_name,

    COUNT(f.result_id) AS total_finishers,
    AVG(e.year_of_event - a.athlete_year_of_birth) AS avg_age
FROM fct_results f
LEFT JOIN dim_events e ON f.event_id = e.event_id
LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
LEFT JOIN dim_country c ON a.athlete_country_code = c.country_code
WHERE a.athlete_gender IS NOT NULL
GROUP BY
    e.event_unit_type,
    e.event_name,
    a.athlete_gender,
    a.athlete_country_code,
    c.country_name
ORDER BY total_finishers DESC;

