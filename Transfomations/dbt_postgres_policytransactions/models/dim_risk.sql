{{ config(materialized='view') }}

select * from {{ref('dim_risk_io')}}
union all
select * from {{ref('dim_risk_bt')}}