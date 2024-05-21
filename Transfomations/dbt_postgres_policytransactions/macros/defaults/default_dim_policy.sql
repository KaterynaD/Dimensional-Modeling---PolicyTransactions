{% macro default_dim_policy() %}

select
0 policy_id,
0 policy_uniqueid,
'Unknown' policynumber,
'1900-01-01' effectivedate,
'2900-01-01' expirationdate,
'1900-01-01' inceptiondate,
'~' policystate,
'~' carriercd,
'~' companycd,
{{ loaddate() }}
union all

{% endmacro %}