WITH weekly_accident_rate AS (
	SELECT
		TO_CHAR(DATE_TRUNC('week', report_date), 'IYYY-MM-W') AS week,
		ROUND(SUM(accident_fatalities + accident_injuries)::NUMERIC / SUM(road_accidents), 2) AS accident_pct
	FROM public.police_reports
	GROUP BY 1
	ORDER BY 1
)

SELECT
	week,
	ROUND(
        COALESCE(
            (accident_pct - LAG(accident_pct) OVER(ORDER BY week ASC)) / NULLIF(LAG(accident_pct) OVER(ORDER BY week ASC), 0), 0) * 100, 2) AS pct_change
FROM weekly_accident_rate
