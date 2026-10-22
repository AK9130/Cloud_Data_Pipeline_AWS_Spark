-- Top 10 states with most accidents
SELECT State, COUNT(*) AS accident_count
FROM accidents_clean_parquet
GROUP BY State
ORDER BY accident_count DESC
LIMIT 10;

-- Accidents by severity
SELECT Severity, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
GROUP BY Severity
ORDER BY Severity;

-- Accidents by weather condition (cleaned NULLs)
SELECT Weather_Condition, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
WHERE Weather_Condition IS NOT NULL
GROUP BY Weather_Condition
ORDER BY total_accidents DESC
LIMIT 10;

-- Top 10 cities in California
SELECT City, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
WHERE State = 'CA'
GROUP BY City
ORDER BY total_accidents DESC
LIMIT 10;

-- Accidents per hour (0 to 23 order)
SELECT EXTRACT(hour FROM Start_Time) AS accident_hour, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
GROUP BY EXTRACT(hour FROM Start_Time)
ORDER BY accident_hour;

-- Accident count per minute using subquery
SELECT minute, COUNT(*) AS total_accidents
FROM (
    SELECT EXTRACT(minute FROM Start_Time) AS minute
    FROM accidents_clean_parquet
) t
GROUP BY minute
ORDER BY total_accidents DESC
LIMIT 10;

-- Month-wise accidents
SELECT MONTH(Start_Time) AS month, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
GROUP BY MONTH(Start_Time)
ORDER BY month;

-- Top 5 states with highest average severity (small states filtered out)
SELECT State, ROUND(AVG(Severity), 2) AS avg_severity, COUNT(*) AS total_accidents
FROM accidents_clean_parquet
GROUP BY State
HAVING COUNT(*) > 1000
ORDER BY avg_severity DESC
LIMIT 5;

-- Top 3 cities per state (window + CTE)
WITH city_counts AS (
    SELECT State, City, COUNT(*) AS total_accidents
    FROM accidents_clean_parquet
    GROUP BY State, City
)
SELECT State, City, total_accidents
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY State ORDER BY total_accidents DESC) AS city_rank
    FROM city_counts
) t
WHERE city_rank <= 3
ORDER BY State, city_rank;
