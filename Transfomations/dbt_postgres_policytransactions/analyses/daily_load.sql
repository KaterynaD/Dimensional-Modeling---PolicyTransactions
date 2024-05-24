/*This is a union all of the loaded records on a specific loaddate from dims and facts tables*/

{% set dims = list_of_models_by_name('dim') %}

{% set facts = list_of_models_by_name('fact') %}


with loaded_on_date as (
{% for t in dims|list + facts|list %}

{% if  t!='dim_transaction' %}

select 
'{{t}}' table_name,
count(*) cnt_records_on,
{{ loaddate() }}
from 
{{ database }}.{% if  target.schema==schema %}{{ target.schema }}.{% else %}{{ target.schema }}_{{ schema }}.{% endif %}{{t}}
where {{ loaddate_in_where() }}      

{% if not loop.last %} union all {% endif %}

{% endif %}



{% endfor %}
)
,totals as (
{% for t in dims|list + facts|list %}

{% if  t!='dim_transaction' %}

select 
'{{t}}' table_name,
count(*) total_cnt_records,
max(loaddate) latest_loaded_date
from 
{{ database }}.{% if  target.schema==schema %}{{ target.schema }}.{% else %}{{ target.schema }}_{{ schema }}.{% endif %}{{t}}
      

{% if not loop.last %} union all {% endif %}

{% endif %}



{% endfor %}
)
select
lod.table_name,
lod.cnt_records_on,
lod.loaddate,
t.total_cnt_records,
t.latest_loaded_date
from loaded_on_date lod
join totals t
on lod.table_name=t.table_name

