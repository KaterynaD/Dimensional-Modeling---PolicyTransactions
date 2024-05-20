{% macro default_dim_risk() %}

select
0 policy_uniqueid,
'1900-01-01' transactioneffectivedate,
'Unknown' risk_uniqueid,
'~' vin,
'~' model,
'~' modelyr,
'~' manufacturer,
{{ loaddate() }}
union all

{% endmacro %}