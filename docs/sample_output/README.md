# Sample output

A snapshot of what the pipeline produces, exported from the Postgres warehouse by `src/export_showcase.py` (Prefect deployment `ecommerce-export-showcase/run`). The Streamlit dashboard shows the same datasets. Regenerate it after a run and commit it to refresh this page.

- Exported: 2026-10-03 15:36 UTC
- Orders watermark: 2018-08-30 23:44:41

## Row counts per layer

| table | rows |
|---|---|
| source.orders | 20000 |
| source.customers | 20000 |
| staging.orders | 20000 |
| staging.order_items | 25024 |
| analytics.fact_orders | 25024 |
| analytics.fact_payments | 20000 |
| analytics.dim_customer | 20000 |
| analytics.dim_product | 2000 |
| analytics.dim_seller | 200 |
| analytics.agg_daily_sales | 607 |
| analytics.agg_monthly_kpis | 20 |
| analytics.agg_customer_retention | 210 |
| analytics.agg_product_sales | 2000 |

## Business

### Monthly KPIs: orders, new vs returning customers, revenue, average order value

Full file: [kpi_monthly.csv](kpi_monthly.csv) (20 rows)

| month | orders | customers | new_customers | returning_customers | items_sold | revenue | freight | avg_order_value | late_delivery_pct |
|---|---|---|---|---|---|---|---|---|---|
| 2017-01 | 993 | 986 | 986 | 0 | 1276.0 | 128543.09 | 28898.99 | 129.45 | 22.6 |
| 2017-02 | 894 | 888 | 871 | 17 | 1134.0 | 106670.91 | 25694.84 | 119.32 | 22.8 |
| 2017-03 | 1054 | 1042 | 1005 | 37 | 1294.0 | 121757.49 | 28913.07 | 115.52 | 21.1 |
| 2017-04 | 953 | 941 | 889 | 52 | 1175.0 | 111124.65 | 26605.98 | 116.61 | 20.4 |
| 2017-05 | 1026 | 1013 | 920 | 93 | 1285.0 | 122082.04 | 28672.93 | 118.99 | 22.6 |
| 2017-06 | 947 | 937 | 841 | 96 | 1197.0 | 113130.04 | 27556.88 | 119.46 | 20.6 |
| 2017-07 | 993 | 984 | 865 | 119 | 1249.0 | 115831.0 | 27734.7 | 116.65 | 23.0 |
| 2017-08 | 923 | 916 | 791 | 125 | 1119.0 | 108772.96 | 25011.97 | 117.85 | 20.2 |
| 2017-09 | 953 | 943 | 812 | 131 | 1189.0 | 113209.6 | 26638.17 | 118.79 | 23.2 |
| 2017-10 | 993 | 982 | 808 | 174 | 1229.0 | 117367.54 | 26882.3 | 118.19 | 22.4 |
| 2017-11 | 953 | 948 | 757 | 191 | 1220.0 | 114749.25 | 27528.11 | 120.41 | 21.8 |
| 2017-12 | 957 | 941 | 752 | 189 | 1207.0 | 110939.33 | 27329.25 | 115.92 | 20.9 |
| 2018-01 | 1056 | 1047 | 799 | 248 | 1309.0 | 125065.06 | 29260.29 | 118.43 | 24.1 |
| 2018-02 | 908 | 903 | 687 | 216 | 1137.0 | 107116.56 | 26106.0 | 117.97 | 21.3 |
| 2018-03 | 962 | 958 | 714 | 244 | 1195.0 | 114942.63 | 26977.87 | 119.48 | 22.1 |

### Daily revenue, orders and average order value

Full file: [daily_sales.csv](daily_sales.csv) (607 rows)

| date_day | orders | items_sold | revenue | freight | late_deliveries | avg_order_value |
|---|---|---|---|---|---|---|
| 2017-01-01 | 39 | 50 | 4825.01 | 1272.4 | 9 | 123.72 |
| 2017-01-02 | 29 | 41 | 4332.59 | 992.43 | 3 | 149.4 |
| 2017-01-03 | 32 | 38 | 3046.33 | 828.64 | 5 | 95.2 |
| 2017-01-04 | 43 | 52 | 5556.21 | 1276.45 | 14 | 129.21 |
| 2017-01-05 | 22 | 25 | 2288.39 | 591.38 | 6 | 104.02 |
| 2017-01-06 | 30 | 34 | 3398.2 | 673.84 | 7 | 113.27 |
| 2017-01-07 | 36 | 43 | 3700.09 | 977.41 | 13 | 102.78 |
| 2017-01-08 | 29 | 33 | 3806.28 | 671.03 | 8 | 131.25 |
| 2017-01-09 | 41 | 51 | 5622.22 | 1188.28 | 11 | 137.13 |
| 2017-01-10 | 36 | 56 | 5530.58 | 1205.39 | 18 | 153.63 |
| 2017-01-11 | 28 | 35 | 3355.3 | 733.61 | 7 | 119.83 |
| 2017-01-12 | 29 | 38 | 3427.25 | 850.45 | 10 | 118.18 |
| 2017-01-13 | 29 | 38 | 4436.95 | 861.97 | 8 | 153.0 |
| 2017-01-14 | 35 | 47 | 5176.58 | 1064.7 | 4 | 147.9 |
| 2017-01-15 | 29 | 42 | 4258.71 | 886.79 | 11 | 146.85 |

### Monthly cohort retention (% of a cohort ordering again N months later)

Full file: [customer_retention.csv](customer_retention.csv) (210 rows)

| cohort_month | months_since_first | cohort_size | active_customers | retention_pct |
|---|---|---|---|---|
| 2017-01 | 0 | 986 | 986 | 100.0 |
| 2017-01 | 1 | 986 | 17 | 1.7 |
| 2017-01 | 2 | 986 | 25 | 2.5 |
| 2017-01 | 3 | 986 | 15 | 1.5 |
| 2017-01 | 4 | 986 | 27 | 2.7 |
| 2017-01 | 5 | 986 | 17 | 1.7 |
| 2017-01 | 6 | 986 | 22 | 2.2 |
| 2017-01 | 7 | 986 | 17 | 1.7 |
| 2017-01 | 8 | 986 | 16 | 1.6 |
| 2017-01 | 9 | 986 | 15 | 1.5 |
| 2017-01 | 10 | 986 | 22 | 2.2 |
| 2017-01 | 11 | 986 | 21 | 2.1 |
| 2017-01 | 12 | 986 | 22 | 2.2 |
| 2017-01 | 13 | 986 | 15 | 1.5 |
| 2017-01 | 14 | 986 | 13 | 1.3 |

### Top 50 products by revenue

Full file: [product_sales_top.csv](product_sales_top.csv) (50 rows)

| revenue_rank | product_id | category | orders | units_sold | revenue | avg_price | revenue_share_pct | first_sale | last_sale |
|---|---|---|---|---|---|---|---|---|---|
| 1 | prd000673 | garden_tools | 20 | 20 | 2556.48 | 127.82 | 0.11 | 2017-01-17 | 2018-08-09 |
| 2 | prd000074 | electronics | 22 | 22 | 2543.22 | 115.6 | 0.109 | 2017-01-13 | 2018-07-18 |
| 3 | prd001086 | baby | 25 | 25 | 2495.03 | 99.8 | 0.107 | 2017-01-11 | 2018-08-17 |
| 4 | prd001554 | computers | 20 | 20 | 2481.47 | 124.07 | 0.106 | 2017-01-20 | 2018-08-01 |
| 5 | prd000091 | unknown | 22 | 22 | 2430.94 | 110.5 | 0.104 | 2017-02-06 | 2018-08-28 |
| 6 | prd000788 | unknown | 18 | 18 | 2426.62 | 134.81 | 0.104 | 2017-01-15 | 2018-07-30 |
| 7 | prd001856 | sports_leisure | 20 | 20 | 2409.86 | 120.49 | 0.103 | 2017-01-06 | 2018-05-15 |
| 8 | prd001830 | computers | 14 | 14 | 2390.58 | 170.76 | 0.102 | 2017-01-16 | 2018-08-12 |
| 9 | prd000653 | toys | 23 | 23 | 2385.14 | 103.7 | 0.102 | 2017-05-18 | 2018-08-26 |
| 10 | prd000483 | health_beauty | 18 | 18 | 2337.4 | 129.86 | 0.1 | 2017-01-16 | 2018-07-17 |
| 11 | prd000197 | furniture | 22 | 22 | 2328.26 | 105.83 | 0.1 | 2017-01-21 | 2018-08-28 |
| 12 | prd001117 | pet_shop | 19 | 19 | 2318.51 | 122.03 | 0.099 | 2017-02-06 | 2018-05-23 |
| 13 | prd001494 | sports_leisure | 20 | 20 | 2307.02 | 115.35 | 0.099 | 2017-02-28 | 2018-08-24 |
| 14 | prd000394 | auto | 15 | 15 | 2303.01 | 153.53 | 0.099 | 2017-01-05 | 2018-08-19 |
| 15 | prd001380 | books | 18 | 18 | 2298.7 | 127.71 | 0.099 | 2017-01-09 | 2018-08-07 |

### Revenue, orders and average delivery days per product category

Full file: [agg_category_revenue.csv](agg_category_revenue.csv) (16 rows)

| category | orders | revenue | avg_delivery_days |
|---|---|---|---|
| perfumery | 1707 | 165730.09 | 14.0 |
| watches_gifts | 1670 | 163978.85 | 13.7 |
| books | 1631 | 159108.44 | 13.9 |
| auto | 1631 | 158687.18 | 14.1 |
| baby | 1610 | 155487.11 | 13.8 |
| electronics | 1634 | 154726.76 | 14.0 |
| furniture | 1578 | 154560.22 | 13.9 |
| garden_tools | 1593 | 153927.56 | 13.8 |
| toys | 1592 | 151488.2 | 14.0 |
| health_beauty | 1563 | 151130.27 | 13.7 |
| computers | 1550 | 147979.59 | 14.2 |
| sports_leisure | 1484 | 146021.01 | 13.9 |
| pet_shop | 1466 | 144097.84 | 14.3 |
| bed_bath_table | 1463 | 143334.78 | 13.9 |
| housewares | 1418 | 132524.86 | 14.1 |

## Operations

### Pipeline run history (latest 200 flow runs)

Full file: [flow_runs.csv](flow_runs.csv) (11 rows)

| pipeline_run_id | flow_name | status | start_ts | end_ts | duration_seconds | task_attempts | failed_attempts | retried_attempts | first_failed_task | rows_inserted | rows_updated | rows_rejected | error |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | ecommerce-daily | SUCCESS | 2026-10-03 15:26:15.080565 | 2026-10-03 15:36:01.859016 | 587.079 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| cd92d289-149f-427c-99bf-f3564a5f76eb | ecommerce-lake-maintenance | SUCCESS | 2026-10-03 13:56:57.789726 | 2026-10-03 14:01:56.790781 | 299.242 | 1 | 0 | 0 | None | 0.0 | 0.0 | 0.0 | None |
| cf274057-8db8-4900-9515-07664cbda687 | ecommerce-lake-maintenance | SUCCESS | 2026-10-03 13:01:49.620752 | 2026-10-03 13:47:27.995144 | 2739.276 | 1 | 0 | 0 | None | 0.0 | 0.0 | 0.0 | None |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | ecommerce-daily | SUCCESS | 2026-10-03 12:48:12.694792 | 2026-10-03 12:59:24.758153 | 672.519 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| 39d661af-79e9-4d61-bccb-b3ccaffb5209 | ecommerce-daily | SUCCESS | 2026-10-03 09:34:51.573639 | 2026-10-03 09:36:52.161979 | 120.813 | 2 | 0 | 0 | None | 0.0 | 0.0 | 0.0 | None |
| 68c9ddf1-c91e-4125-af5f-a6c858efb1c4 | ecommerce-daily | SUCCESS | 2026-10-03 08:58:15.898975 | 2026-10-03 09:16:45.230657 | 1111.616 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| 9c76a991-e108-4694-850b-2845e5ae5d69 | ecommerce-daily | SUCCESS | 2026-10-03 08:22:05.825627 | 2026-10-03 08:38:44.351059 | 999.16 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| ea5b2a8d-6b79-46d9-9968-a630749851e1 | ecommerce-daily | SUCCESS | 2026-10-03 08:12:57.960097 | 2026-10-03 08:21:43.579708 | 526.019 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| 66220c8a-ff6a-4185-8b57-cdcd2f13e4f3 | ecommerce-daily | SUCCESS | 2026-10-03 05:44:59.534776 | 2026-10-03 07:41:34.441038 | 565.255 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| eb670ba0-2608-4f8c-8888-c15f6548e532 | ecommerce-daily | SUCCESS | 2026-10-03 05:19:07.429088 | 2026-10-03 05:26:56.551370 | 469.274 | 5 | 0 | 0 | None | 221672.0 | 0.0 | 0.0 | None |
| a24aff99-1c8b-40ac-8fa2-a9dc250ba736 | ecommerce-daily | SUCCESS | 2026-10-03 05:09:39.023233 | 2026-10-03 05:17:25.756351 | 466.976 | 5 | 0 | 0 | None | 241672.0 | 0.0 | 0.0 | None |

### Task attempts with durations (latest 1000)

Full file: [task_runs.csv](task_runs.csv) (45 rows)

| pipeline_run_id | task_name | status | start_ts | duration_seconds | retry_count |
|---|---|---|---|---|---|
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | quality-checks | SUCCESS | 2026-10-03 15:34:48.419527 | 73.324 | 0 |
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | dbt-build | SUCCESS | 2026-10-03 15:34:24.361409 | 24.051 | 0 |
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | load-warehouse | SUCCESS | 2026-10-03 15:32:15.966963 | 127.991 | 0 |
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | transform-silver | SUCCESS | 2026-10-03 15:28:17.747541 | 238.031 | 0 |
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | extract-bronze | SUCCESS | 2026-10-03 15:26:15.106879 | 122.204 | 0 |
| cd92d289-149f-427c-99bf-f3564a5f76eb | lake-maintenance | SUCCESS | 2026-10-03 13:56:57.833444 | 298.724 | 0 |
| cf274057-8db8-4900-9515-07664cbda687 | lake-maintenance | SUCCESS | 2026-10-03 13:01:49.671088 | 2738.066 | 0 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | quality-checks | SUCCESS | 2026-10-03 12:57:22.923760 | 121.307 | 0 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | dbt-build | SUCCESS | 2026-10-03 12:57:00.760401 | 23.15 | 0 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | load-warehouse | SUCCESS | 2026-10-03 12:54:57.734244 | 121.301 | 0 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | transform-silver | SUCCESS | 2026-10-03 12:51:07.302307 | 230.252 | 0 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | extract-bronze | SUCCESS | 2026-10-03 12:48:12.754058 | 174.275 | 0 |
| 39d661af-79e9-4d61-bccb-b3ccaffb5209 | quality-checks | SUCCESS | 2026-10-03 09:35:18.082777 | 94.057 | 0 |
| 39d661af-79e9-4d61-bccb-b3ccaffb5209 | dbt-build | SUCCESS | 2026-10-03 09:34:51.600765 | 26.469 | 0 |
| 68c9ddf1-c91e-4125-af5f-a6c858efb1c4 | quality-checks | SUCCESS | 2026-10-03 09:14:48.427864 | 116.385 | 0 |

### Per task: success rate and duration statistics

Full file: [task_stats.csv](task_stats.csv) (7 rows)

| task_name | attempts | successes | failures | success_rate | avg_seconds | median_seconds | p95_seconds | max_seconds | last_start | last_status |
|---|---|---|---|---|---|---|---|---|---|---|
| dbt-build | 9 | 9 | 0 | 100.0 | 22.1 | 20.2 | 39.1 | 47.5 | 2026-10-03 15:34:24.361409 | SUCCESS |
| delta-inspect | 1 | 1 | 0 | 100.0 | 37.3 | 37.3 | 37.3 | 37.3 | 2026-10-03 07:42:15.224553 | SUCCESS |
| extract-bronze | 8 | 8 | 0 | 100.0 | 170.0 | 137.1 | 313.2 | 361.6 | 2026-10-03 15:26:15.106879 | SUCCESS |
| lake-maintenance | 2 | 2 | 0 | 100.0 | 1518.4 | 1518.4 | 2616.1 | 2738.1 | 2026-10-03 13:56:57.833444 | SUCCESS |
| load-warehouse | 8 | 8 | 0 | 100.0 | 133.0 | 118.5 | 207.3 | 215.8 | 2026-10-03 15:32:15.966963 | SUCCESS |
| quality-checks | 9 | 9 | 0 | 100.0 | 89.3 | 83.3 | 125.7 | 128.6 | 2026-10-03 15:34:48.419527 | SUCCESS |
| transform-silver | 8 | 8 | 0 | 100.0 | 259.5 | 234.1 | 401.9 | 408.9 | 2026-10-03 15:28:17.747541 | SUCCESS |

### Rows per table in the latest daily run

Full file: [table_loads_latest.csv](table_loads_latest.csv) (18 rows)

| task_name | table_name | rows_read | inserted | updated | rejected | duration_seconds | status |
|---|---|---|---|---|---|---|---|
| extract-bronze | bronze.orders | 2 | 0 | 0 | 0 | 41.861 | SUCCESS |
| extract-bronze | bronze.customers | 20000 | 20000 | 0 | 0 | 22.085 | SUCCESS |
| extract-bronze | bronze.products | 2000 | 2000 | 0 | 0 | 10.096 | SUCCESS |
| extract-bronze | bronze.sellers | 200 | 200 | 0 | 0 | 10.458 | SUCCESS |
| extract-bronze | bronze.order_items | 25024 | 25024 | 0 | 0 | 13.216 | SUCCESS |
| extract-bronze | bronze.order_payments | 20000 | 20000 | 0 | 0 | 11.444 | SUCCESS |
| transform-silver | silver.orders | 0 | 0 | 0 | 0 | 22.549 | SUCCESS |
| transform-silver | silver.customers | 20000 | 20000 | 0 | 0 | 65.702 | SUCCESS |
| transform-silver | silver.products | 2000 | 2000 | 0 | 0 | 30.858 | SUCCESS |
| transform-silver | silver.sellers | 200 | 200 | 0 | 0 | 42.054 | SUCCESS |
| transform-silver | silver.order_items | 25024 | 25024 | 0 | 0 | 30.471 | SUCCESS |
| transform-silver | silver.order_payments | 20000 | 20000 | 0 | 0 | 30.425 | SUCCESS |
| load-warehouse | staging.orders | 20000 | 20000 | 0 | 0 | 50.129 | SUCCESS |
| load-warehouse | staging.customers | 20000 | 20000 | 0 | 0 | 16.455 | SUCCESS |
| load-warehouse | staging.products | 2000 | 2000 | 0 | 0 | 8.703 | SUCCESS |

## Data quality

### Data quality score per run (latest 100 runs)

Full file: [dq_scorecard.csv](dq_scorecard.csv) (10 rows)

| run_id | run_ts | checks | passed | warned | failed | pass_rate | health_score |
|---|---|---|---|---|---|---|---|
| d4ca61c4-ff04-46dd-8a90-ffb9a0011415 | 2026-10-03 15:34:41.610191 | 90 | 89 | 1 | 0 | 98.9 | 99.4 |
| 5e5432d9-40ed-4b09-8c2c-f8d343b69660 | 2026-10-03 12:57:17.040788 | 75 | 74 | 1 | 0 | 98.7 | 99.3 |
| 39d661af-79e9-4d61-bccb-b3ccaffb5209 | 2026-10-03 09:35:11.862408 | 75 | 74 | 1 | 0 | 98.7 | 99.3 |
| 68c9ddf1-c91e-4125-af5f-a6c858efb1c4 | 2026-10-03 09:14:40.186126 | 75 | 74 | 1 | 0 | 98.7 | 99.3 |
| 9c76a991-e108-4694-850b-2845e5ae5d69 | 2026-10-03 08:38:42.872160 | 8 | 7 | 1 | 0 | 87.5 | 93.8 |
| ea5b2a8d-6b79-46d9-9968-a630749851e1 | 2026-10-03 08:21:42.263481 | 8 | 7 | 1 | 0 | 87.5 | 93.8 |
| 66220c8a-ff6a-4185-8b57-cdcd2f13e4f3 | 2026-10-03 07:41:32.857541 | 7 | 6 | 1 | 0 | 85.7 | 92.9 |
| eb670ba0-2608-4f8c-8888-c15f6548e532 | 2026-10-03 05:26:53.825696 | 7 | 6 | 1 | 0 | 85.7 | 92.9 |
| a24aff99-1c8b-40ac-8fa2-a9dc250ba736 | 2026-10-03 05:17:24.535679 | 7 | 6 | 1 | 0 | 85.7 | 92.9 |
| 0f3b40b6-47e4-4d2d-b11a-f6258f416ea6 | 2026-10-02 11:28:31.299928 | 7 | 7 | 0 | 0 | 100.0 | 100.0 |

### Latest run: checks per category

Full file: [dq_by_category_latest.csv](dq_by_category_latest.csv) (7 rows)

| category | checks | passed | warned | failed | pass_rate |
|---|---|---|---|---|---|
| business_rule | 3 | 3 | 0 | 0 | 100.0 |
| completeness | 27 | 27 | 0 | 0 | 100.0 |
| freshness | 7 | 6 | 1 | 0 | 85.7 |
| reconciliation | 10 | 10 | 0 | 0 | 100.0 |
| referential_integrity | 8 | 8 | 0 | 0 | 100.0 |
| uniqueness | 19 | 19 | 0 | 0 | 100.0 |
| validity | 16 | 16 | 0 | 0 | 100.0 |

### Latest run: every check (pipeline checks and dbt tests)

Full file: [dq_checks_latest.csv](dq_checks_latest.csv) (90 rows)

| check_name | check_source | category | status | rows_failed | detail | run_ts |
|---|---|---|---|---|---|---|
| dbt_freshness:staging.orders | dbt | freshness | WARN | 0 | max_loaded_at=2026-10-02T03:56:04.370200+00:00 age_hours=35.6 | 2026-10-03 15:34:48.270659 |
| dbt:assert_business_aggregates_reconcile | dbt | business_rule | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:assert_fact_orders_complete | dbt | business_rule | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:assert_revenue_reconciles | dbt | business_rule | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_agg_customer_retention_months_since_first | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_agg_customer_retention_retention_pct | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_agg_monthly_kpis_avg_order_value | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_agg_monthly_kpis_month | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_agg_product_sales_product_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_dim_customer_customer_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_dim_customer_is_current | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_dim_date_date_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_dim_product_product_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_dim_seller_seller_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |
| dbt:not_null_fact_orders_customer_key | dbt | completeness | PASS | 0 |  | 2026-10-03 15:34:41.610191 |

### Rows rejected by Silver validation, per rule (latest 200)

Full file: [rejected_records.csv](rejected_records.csv) (0 rows)

_No rows._

### Detected schema changes and what the pipeline did (latest 50)

Full file: [schema_changes.csv](schema_changes.csv) (12 rows)

| detected_at | table_name | change_type | column_name | old_type | new_type | action |
|---|---|---|---|---|---|---|
| 2026-10-03 07:40:13.295618 | staging.order_payments | added | source_table | None | string | evolved |
| 2026-10-03 07:40:13.293329 | staging.order_payments | added | pipeline_run_id | None | string | evolved |
| 2026-10-03 07:40:04.476575 | staging.order_items | added | source_table | None | string | evolved |
| 2026-10-03 07:40:04.473513 | staging.order_items | added | pipeline_run_id | None | string | evolved |
| 2026-10-03 07:39:58.909115 | staging.sellers | added | source_table | None | string | evolved |
| 2026-10-03 07:39:58.905936 | staging.sellers | added | pipeline_run_id | None | string | evolved |
| 2026-10-03 07:39:52.074208 | staging.products | added | source_table | None | string | evolved |
| 2026-10-03 07:39:52.071192 | staging.products | added | pipeline_run_id | None | string | evolved |
| 2026-10-03 07:39:43.392304 | staging.customers | added | source_table | None | string | evolved |
| 2026-10-03 07:39:43.388788 | staging.customers | added | pipeline_run_id | None | string | evolved |
| 2026-10-03 07:39:23.583003 | staging.orders | added | source_table | None | string | evolved |
| 2026-10-03 07:39:23.571869 | staging.orders | added | pipeline_run_id | None | string | evolved |

## Model

### SCD Type 2: customers with more than one version

Full file: [dim_customer_scd2_examples.csv](dim_customer_scd2_examples.csv) (0 rows)

_No rows._

### Sample of 50 fact rows (grain: one row per order item)

Full file: [fact_orders_sample.csv](fact_orders_sample.csv) (50 rows)

| order_id | order_item_id | date_key | order_status | order_purchase_timestamp | price | freight_value | delivery_days | is_late |
|---|---|---|---|---|---|---|---|---|
| ord00018483 | 1 | 20180830 | delivered | 2018-08-30 23:44:41 | 194.54 | 7.38 | 5.0 | 0 |
| ord00009701 | 1 | 20180830 | processing | 2018-08-30 23:38:05 | 38.93 | 11.0 | nan | 0 |
| ord00017734 | 1 | 20180830 | delivered | 2018-08-30 23:18:43 | 66.87 | 9.28 | 14.0 | 1 |
| ord00000310 | 1 | 20180830 | delivered | 2018-08-30 23:13:53 | 173.23 | 10.11 | 18.0 | 0 |
| ord00000310 | 2 | 20180830 | delivered | 2018-08-30 23:13:53 | 61.6 | 29.57 | 18.0 | 0 |
| ord00011806 | 1 | 20180830 | delivered | 2018-08-30 23:13:20 | 238.27 | 37.0 | 5.0 | 0 |
| ord00016150 | 1 | 20180830 | delivered | 2018-08-30 23:05:51 | 211.17 | 33.72 | 14.0 | 0 |
| ord00016150 | 2 | 20180830 | delivered | 2018-08-30 23:05:51 | 115.09 | 10.07 | 14.0 | 0 |
| ord00012946 | 1 | 20180830 | delivered | 2018-08-30 22:41:15 | 100.46 | 32.24 | 7.0 | 0 |
| ord00003672 | 1 | 20180830 | delivered | 2018-08-30 21:28:56 | 153.14 | 25.45 | 4.0 | 0 |
| ord00006859 | 1 | 20180830 | delivered | 2018-08-30 20:30:10 | 83.45 | 10.28 | 9.0 | 0 |
| ord00003515 | 1 | 20180830 | delivered | 2018-08-30 19:49:43 | 26.55 | 10.1 | 19.0 | 0 |
| ord00003515 | 2 | 20180830 | delivered | 2018-08-30 19:49:43 | 45.39 | 23.92 | 19.0 | 0 |
| ord00008570 | 1 | 20180830 | delivered | 2018-08-30 19:48:29 | 176.85 | 26.27 | 4.0 | 0 |
| ord00005219 | 1 | 20180830 | delivered | 2018-08-30 19:32:38 | 81.27 | 24.8 | 9.0 | 0 |
