SELECT 
	report_date,
	ROUND(
        AVG(road_accidents) OVER(ORDER BY report_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS weekly_rolling_accidents
FROM public.police_reports;