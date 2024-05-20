{{ config(materialized='incremental',
  unique_key=['loaddate']) }}

  select
  '{{ var('loaddate' ) }}'::timestamp without time zone  loaddate,
  '{{ var('new_transactiondate' ) }}'::int transactiondate,
  null::timestamp without time zone as endloaddate