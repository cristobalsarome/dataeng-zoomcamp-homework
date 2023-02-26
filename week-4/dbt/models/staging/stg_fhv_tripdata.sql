{{ config(materialized='view') }}
 
with tripdata as 
(
  select *,
  row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','fhv') }}
  --where vendorid is not null 
)
select
   *
from tripdata


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

