USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_top_countries
    COMMENT "Serving view for top countries" AS
SELECT
    a.athlete_country_code,
    e.event_unit_type,
    e.event_name,

    c.country_name,

    COUNT(f.result_id) AS total_finishers
FROM fct_results f
LEFT JOIN dim_events e  ON f.event_id = e.event_id
LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
LEFT JOIN dim_country c ON a.athlete_country_code = c.country_code
WHERE a.athlete_country_code IS NOT NULL 
GROUP BY a.athlete_country_code, e.event_unit_type, e.event_name, c.country_name
ORDER BY total_finishers DESC;