 {% macro load_scd2(dim_table_name,stg_data,dim_primary_key, dim_unique_key, dim_change_date,history_tracking_columns) %} 

 /*assuming all business-info columns in the table should track history
   it's possible to have business-info columns which should be just updated as in SCD1
   but more aguments and updates is needed in the code
 */

 {% set insert_changed_data %}

/*-------------------Forward Dated Changes-------------------------*/



/*only new changed data*/
create temporary table new_data as
with stg_data as (
/*--new data in stage can have changes for the same uniqueid - historical load as an example --*/
select distinct
{{dim_primary_key}},
{{  dim_change_date }},
{{ dim_unique_key }},
{% for c in history_tracking_columns -%} 
stg.{{ c }} {% if not loop.last %} , {% endif %}
{%- endfor -%}
from {{ ref(stg_data) }} stg
)
/*-- we need existing data from dim to compare with new add/not add with/without changes --*/
, existing_data as (
select
dim.{{dim_primary_key}},
dim.valid_fromdate {{  dim_change_date }},
dim.{{ dim_unique_key }},
{% for c in history_tracking_columns -%} 
dim.{{ c }} {% if not loop.last %} , {% endif %}
{%- endfor -%}
from  {{ ref(dim_table_name) }} dim
where dim.{{ dim_unique_key }} in (select stg.{{ dim_unique_key }} from stg_data stg)
)
,data as (
/*---new data ---*/
select stg.* ,
1 new_data
from stg_data stg 
left outer join existing_data dim 
on stg.{{dim_primary_key}} = dim.{{dim_primary_key}}
where  dim.{{dim_primary_key}} is null
union all
/*--existing data for comparizon*/
select *,
0 new_data from existing_data
)
,history_data as (
select
*,
/*-----------------------------------------------------------*/
coalesce(lead({{  dim_change_date }}) over (partition by {{ dim_unique_key }} order by {{  dim_change_date }}), '3000-01-01') valid_todate,
/*-----------------------------------------------------------*/
{% for c in history_tracking_columns -%} 
lag({{ c }}) over (partition by {{dim_unique_key}} order by {{dim_change_date}}) lag_{{ c }} {% if not loop.last %} , {% endif %}
{%- endfor -%}
/*-----------------------------------------------------------*/
from data
)
select
{{dim_primary_key}},
{{  dim_change_date }},
valid_todate,
{{ dim_unique_key }},
{% for c in history_tracking_columns -%} 
{{ c }} ,
{%- endfor -%}
1 dummy
from history_data
where new_data=1 and
/*for simplicity assuming all columns are varchar, otherwise need to check data type somehow and use appropriate default*/
(  
{% for c in history_tracking_columns -%} 

{{ c }}<>coalesce(lag_{{ c }},'not_found') {% if not loop.last %} or {% endif %} 

{%- endfor -%}


)

order by {{  dim_change_date }};


/*---------------------------Insert New Data---------------------------*/

insert into {{ ref(dim_table_name) }}
(
{{dim_primary_key}},
valid_fromdate,
valid_todate,
{{ dim_unique_key }},
{% for c in history_tracking_columns -%} 
{{ c }} ,
{%- endfor -%}
loaddate
)
select
{{dim_primary_key}},
{{  dim_change_date }} valid_fromdate,
valid_todate,
{{ dim_unique_key }},
{% for c in history_tracking_columns -%} 
{{ c }} ,
{%- endfor -%}
{{ loaddate() }}
from new_data;

/*----------------Adjusting valid_todate - valid_fromdate in a case of backdated transactions*/

with changed_data as (
select
{{dim_primary_key}},
{{ dim_unique_key }},
valid_fromdate,
coalesce(lead(valid_fromdate) over (partition by {{ dim_unique_key }} order by valid_fromdate),'3000-01-01') valid_todate
from {{ ref(dim_table_name) }} dim
where {{ dim_unique_key }} in (select stg.{{ dim_unique_key }} from new_data stg)
)
update {{ ref(dim_table_name) }}
set valid_todate=changed_data.valid_todate
from changed_data
where changed_data.{{dim_primary_key}} = {{ ref(dim_table_name) }}.{{dim_primary_key}};



 {% endset %}




{% do run_query(insert_changed_data) %}


{% endmacro %}