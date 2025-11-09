CREATE VIEW marketing AS
WITH installation AS (
    SELECT
        user_id,
        FIRST_VALUE(event_timestamp) OVER 
        (PARTITION BY user_id ORDER BY event_timestamp ASC) AS install_day
FROM 
    events GROUP BY user_id
),
events_enriched AS (
    SELECT 
        event_timestamp,
        events.tracker_name,
        events.user_id ,
        events.ad_revenue,
        DATE(datetime(installation.install_day/1000000, 'unixepoch', 'localtime')) AS install_day  
    FROM events
    LEFT JOIN installation ON events.user_id = installation.user_id
),
daily_stats AS (
    SELECT 
        install_day,
        tracker_name,
        COUNT(DISTINCT user_id) AS number_of_installs, 
            SUM(ad_revenue) AS total_revenue
    FROM 
        events_enriched 
    GROUP BY 
        install_day, tracker_name 
)
SELECT  
    ua.date , 
    ua.tracker_name ,
    ua.costs ,
    COALESCE(da.number_of_installs , 0) AS number_of_installs,
    COALESCE(da.total_revenue , 0 ) AS total_revenue
FROM 
    user_acquisition AS ua
LEFT JOIN daily_stats AS da 
    ON da.install_day = ua.date 
    AND da.tracker_name = ua.tracker_name
