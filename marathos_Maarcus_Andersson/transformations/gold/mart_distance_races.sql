USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_distance_races
    COMMENT "Serving view, distance races (km and mi)" AS
    SELECT
        f.result_id,
        f.athlete_performance,
        e.event_name,
        e.event_unit_type,
        e.event_start_date,
        e.event_end_date,
        e.year_of_event,
        e.event_number_of_finishers,
        a.athlete_id,
        a.athlete_gender,
        a.athlete_country_code,
        c.country_name,
        a.athlete_age_category,
        a.athlete_year_of_birth
    FROM fct_results f
    LEFT JOIN dim_events e  ON f.event_id = e.event_id
    LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
    LEFT JOIN dim_country c ON a.athlete_country_code = c.country_code
    WHERE e.event_unit_type IN ('km', 'mi');

    

