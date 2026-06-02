USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_timed_races
    COMMENT "Serving view - timed races (h)" AS
    SELECT
        f.result_id,
        f.athlete_performance,
        
        e.event_name,
        e.year_of_event,
        e.event_number_of_finishers,
        e.event_start_date,
        e.event_end_date,

        a.athlete_year_of_birth,
        a.athlete_gender,
        a.athlete_country_code,
        a.athlete_age_category, 

        c.country_name

    FROM fct_results f
    LEFT JOIN dim_events e ON f.event_id = e.event_id
    LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
    LEFT JOIN dim_country c ON a.athlete_country_code = c.country_code
    WHERE e.event_unit_type = 'h';
