USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_swedish_races
    COMMENT "Serving view for Swedish hosted races" AS
SELECT
    f.result_id,
    f.athlete_performance,
    e.event_name,
    e.event_unit_type,
    e.event_distance_km,
    e.host_country_code,
    e.year_of_event,
    e.event_number_of_finishers,
    a.athlete_id,
    e.event_start_date,
    e.event_end_date,
    a.athlete_gender,
    a.athlete_country_code,
    c.country_name,
    a.athlete_age_category,
    a.athlete_year_of_birth
FROM
    fct_results f
    LEFT JOIN dim_events e ON f.event_id = e.event_id
    LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
    LEFT JOIN dim_country c ON a.athlete_country_code = c.country_code
WHERE
    e.host_country_code = 'SWE'