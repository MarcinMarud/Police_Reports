SELECT
	ROUND(SUM(accident_fatalities)::NUMERIC / SUM(road_accidents), 2) AS mortality
FROM public.police_reports;