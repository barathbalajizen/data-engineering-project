-- DIMENSION: date
-- What:  One row per calendar day from 2016-01-01 to 2030-12-31. Generated here, not loaded from a source.
-- Why:   Reports can group by year, month, weekday or weekend with a simple join, instead of repeating date
--        functions in every query.
-- Key:   date_key is the date written as a number: 2017-03-15 -> 20170315. The facts store the same number.
select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d::date                              as date_day,
    extract(year  from d)::int           as year,
    extract(month from d)::int           as month,
    to_char(d, 'Mon')                    as month_name,
    extract(isodow from d)::int          as day_of_week,
    (extract(isodow from d) in (6, 7))   as is_weekend
from generate_series(timestamp '2016-01-01', timestamp '2030-12-31', interval '1 day') as g(d)
