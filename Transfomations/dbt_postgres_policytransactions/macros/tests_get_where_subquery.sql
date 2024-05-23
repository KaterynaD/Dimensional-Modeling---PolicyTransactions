{% macro get_where_subquery(relation) -%}
    {% set where = config.get('where') %}
    {% if where %}
        {% if "__latest_load__" in where %}
            {# replace placeholder string with result of custom macro #}
            {% set latest_load = ["(select max(t.loaddate)  from ",relation," t)"]|join(" ") %}
            {% set where = where | replace("__latest_load__", latest_load) %}
        {% endif %}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {% do return(filtered) %}
    {%- else -%}
        {% do return(relation) %}
    {%- endif -%}
{%- endmacro %}