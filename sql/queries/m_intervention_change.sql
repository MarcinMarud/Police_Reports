WITH monthly_interventions AS (
	SELECT
		TO_CHAR(DATE_TRUNC('month', report_date), 'IYYY-MM') AS month,
		SUM(interventions) AS interventions
	FROM public.police_reports
	GROUP BY 1
),
prev_month AS (
	SELECT
		month,
		interventions,
		COALESCE(LAG(interventions) OVER(ORDER BY month ASC), 0) AS previous_month_interventions
	FROM monthly_interventions
)

SELECT
    *,
    ROUND(
        COALESCE(
            ((interventions - previous_month_interventions) * 100.0 / NULLIF(previous_month_interventions, 0)), 0), 2) AS pct_change
FROM prev_month;