-- Top 10 states with most accidents
SELECT State, COUNT(*) AS accident_count
FROM emr_state_wise_parquet
GROUP BY State
ORDER BY accident_count DESC
LIMIT 10;

-- Accidents by severity
SELECT Severity, COUNT(*) AS total_accidents
FROM emr_state_wise_parquet
GROUP BY Severity
ORDER BY Severity;

-- Accidents by weather condition (cleaned NULLs)
SELECT Weather_Condition, COUNT(*) AS total_accidents
FROM emr_state_wise_parquet
WHERE Weather_Condition IS NOT NULL
GROUP BY Weather_Condition
ORDER BY total_accidents DESC
LIMIT 10;

-- Accidents by city
SELECT City, COUNT(*) AS total_accidents
FROM emr_state_wise_parquet
GROUP BY City
ORDER BY total_accidents DESC
LIMIT 10;

-- Accidents per hour
SELECT EXTRACT(hour FROM Start_Time) AS hour, COUNT(*) AS total_accidents
FROM emr_state_wise_parquet
GROUP BY hour
ORDER BY total_accidents;

-- Accident Count per Minute using Subquery
SELECT minute, count(*) AS total_accidents
FROM (
    SELECT extract(minute from start_time) AS minute
    FROM emr_state_wise_parquet
) t
GROUP BY minute
ORDER BY total_accidents DESC
LIMIT 10;

-- Top 5 states with highest average severity
SELECT State, AVG(Severity) AS avg_severity
FROM emr_state_wise_parquet
GROUP BY State
ORDER BY avg_severity DESC
LIMIT 5;

-- Top 3 cities per state (window + CTE)
WITH city_counts AS (
    SELECT State, City, COUNT(*) AS total_accidents
    FROM emr_state_wise_parquet
    GROUP BY State, City
)
SELECT State, City, total_accidents
FROM (
    SELECT *,
           RANK() OVER (PARTITION BY State ORDER BY total_accidents DESC) AS rnk
    FROM city_counts
) t
WHERE rnk <= 3;
