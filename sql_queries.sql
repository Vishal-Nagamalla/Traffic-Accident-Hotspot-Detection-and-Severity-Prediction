SELECT
    borough,
    EXTRACT(HOUR FROM crash_datetime) AS hour_of_day,
    COUNT(*) AS total_accidents,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS serious_accidents
FROM accidents
WHERE crash_date BETWEEN DATE '2019-01-01' AND DATE '2019-12-31'
GROUP BY borough, hour_of_day
ORDER BY serious_accidents DESC, total_accidents DESC;

SELECT
    borough,
    COUNT(*) AS total_accidents,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS serious_accidents,
    ROUND(AVG(severity)::NUMERIC, 3) AS serious_fraction,
    ROUND(100.0 * SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS serious_pct
FROM accidents
WHERE borough IS NOT NULL
GROUP BY borough
HAVING COUNT(*) >= 100     
ORDER BY serious_fraction DESC, total_accidents DESC;

SELECT
    EXTRACT(DOW FROM crash_datetime) AS day_of_week,
    CASE EXTRACT(DOW FROM crash_datetime)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    COUNT(*) AS total_accidents,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS serious_accidents,
    ROUND(100.0 * SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS serious_pct
FROM accidents
GROUP BY EXTRACT(DOW FROM crash_datetime)
ORDER BY day_of_week;

SELECT
    zip_code,
    COUNT(*) AS total_accidents,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS serious_accidents,
    ROUND(100.0 * SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) / COUNT(*), 2)
        AS serious_pct
FROM accidents
WHERE zip_code IS NOT NULL
GROUP BY zip_code
HAVING COUNT(*) >= 50
ORDER BY serious_accidents DESC
LIMIT 20;

SELECT
    EXTRACT(YEAR FROM crash_date) AS year,
    EXTRACT(MONTH FROM crash_date) AS month,
    COUNT(*) AS total_accidents,
    SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) AS serious_accidents,
    ROUND(100.0 * SUM(CASE WHEN severity = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS serious_pct
FROM accidents
GROUP BY EXTRACT(YEAR FROM crash_date), EXTRACT(MONTH FROM crash_date)
ORDER BY year DESC, month;