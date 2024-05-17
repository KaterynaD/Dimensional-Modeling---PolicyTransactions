{% snapshot dim_risk %}



{{
    config(
      target_database='airflow',
      target_schema='policy_trn',
      unique_key='risk_uniqueid',
      strategy='timestamp',
      updated_at='transactioneffectivedate'
    )
}}


with original_data as (select distinct
risk_uniqueid,
transactioneffectivedate
from {{ref('stg_risk')}} stg
where stg.{{ loaddate_in_where() }})
,data as (select
risk_uniqueid,
transactioneffectivedate,
row_number() over(partition by risk_uniqueid  order by transactioneffectivedate) rn
from original_data
)
{% if  var('load_defaults')   %}

{{ default_dim_risk() }}

{% endif %}

select distinct
stg.policy_uniqueid,
stg.transactioneffectivedate,
stg.risk_uniqueid,
stg.vin,
stg.model,
stg.modelyr,
stg.manufacturer ,
{{ loaddate() }}
from {{ref('stg_risk')}} stg
join data
on stg.risk_uniqueid=data.risk_uniqueid
and stg.transactioneffectivedate=data.transactioneffectivedate
where stg.{{ loaddate_in_where() }}
and data.rn={{ var('batch_num' ) }}








{% endsnapshot %}