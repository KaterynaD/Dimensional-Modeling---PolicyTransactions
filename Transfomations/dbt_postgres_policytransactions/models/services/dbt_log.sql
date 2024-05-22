{{ config(materialized='incremental',
  unique_key=['startdate']) }}

  select
  '1900-01-01'::timestamp without time zone as startdate,
  'Dummy'::varchar(100) as operation,
  null::timestamp without time zone as enddate