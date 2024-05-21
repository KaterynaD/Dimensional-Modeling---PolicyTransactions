with original_data as (select distinct
risk_uniqueid,
transactioneffectivedate
from  {{ref('stg_risk')}} stg
where stg.{{ loaddate_in_where() }}
)
,data as (select
risk_uniqueid,
transactioneffectivedate,
row_number() over(partition by risk_uniqueid  order by transactioneffectivedate) rn
from original_data
)
select
'dim_risk' table_name,
max(rn) num_batches
from data