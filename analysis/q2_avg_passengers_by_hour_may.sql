-- ============================================================================
-- Q2 — Average passenger_count by hour of day in MAY, ALL taxis (yellow + green)
-- ----------------------------------------------------------------------------
-- "Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram
--  táxi no mês de maio considerando todos os táxis da frota?"
--
-- "All taxis of the fleet" = yellow + green (the two licensed taxi types). FHV /
-- Uber are for-hire vehicles, not taxis, and HVFHV has no passenger_count.
--
-- Source : ifood.gold.taxi_trips   (yellow + green unified)
-- Filters: May 2023; passenger_count >= 1 — NULL/0-passenger trips are metering
--          errors that would bias a per-passenger average.
-- Note   : pickup_datetime is timestamp_ntz (tz-naive), so hour() is the real
--          local NYC pickup hour.
-- ============================================================================

SELECT
    hour(pickup_datetime)          AS hour_of_day,
    round(avg(passenger_count), 3) AS avg_passengers,
    count(*)                       AS trips
FROM ifood.gold.taxi_trips
WHERE year(pickup_datetime) = 2023
  AND month(pickup_datetime) = 5
  AND passenger_count >= 1
GROUP BY hour(pickup_datetime)
ORDER BY hour_of_day;
