SELECT
	ROUND(
		SUM(caught_in_the_act + wanted_persons_arrested)::NUMERIC / SUM(interventions), 2) AS clearance_rate
FROM public.police_reports

