select
    to_char(d, 'YYYYMMDD')::int          as date_key,
    d::date                              as date_day,
    extract(year  from d)::int           as year,
    extract(month from d)::int           as month,
    to_char(d, 'Mon')                    as month_name,
    extract(isodow from d)::int          as day_of_week,
    (extract(isodow from d) in (6, 7))   as is_weekend
from generate_series(timestamp '2016-01-01', timestamp '2030-12-31', interval '1 day') as g(d)
