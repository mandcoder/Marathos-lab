USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_events_calendar
    COMMENT "Serving view with date attributes" AS
SELECT DISTINCT
    
    e.event_name,
    e.host_country_code,
    e.event_start_date,    
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.weekday_name
 
FROM fct_results f

LEFT JOIN marathos.gold.dim_events e  ON f.event_id = e.event_id
LEFT JOIN marathos.gold.dim_date d    ON e.event_start_date = d.date
