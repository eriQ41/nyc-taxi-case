-- ============================================================================
-- Q1 — Average monthly total_amount across all YELLOW taxis
-- ----------------------------------------------------------------------------
-- "Qual a média de valor total (total_amount) recebido em um mês considerando
--  todos os yellow táxis da frota?"
--
-- Interpretation: the average total_amount per trip, grouped by pickup month
-- (one figure per month, Jan–May 2023).
--
-- Source : ifood.gold.yellow_trips_consumption   (yellow only, as the question asks)
-- Filter : total_amount >= 0  — negative totals are refunds/adjustments, not fare
--          actually received, so they are excluded from the revenue average.
-- ============================================================================

SELECT
    date_format(tpep_pickup_datetime, 'yyyy-MM') AS month,
    round(avg(total_amount), 2)                  AS avg_total_amount,
    count(*)                                      AS trips
FROM ifood.gold.yellow_trips_consumption
WHERE total_amount >= 0
GROUP BY date_format(tpep_pickup_datetime, 'yyyy-MM')
ORDER BY month;

-- Optional: single overall average across the whole Jan–May period
-- SELECT round(avg(total_amount), 2) AS avg_total_amount_overall
-- FROM ifood.gold.yellow_trips_consumption
-- WHERE total_amount >= 0;
