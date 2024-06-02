{% macro get_run_at_date() %}

    {# 1. Make our life easier by setting variables for the Python classes. #}
    {% set python_date = modules.datetime.date.fromisoformat %}
    {% set time_delta =  modules.datetime.timedelta %}
    {% set utc = modules.pytz.utc %}
    {% set los_angeles = modules.pytz.timezone('America/Los_Angeles') %}

    {# 2. Check if we passed in a run at date. #}
    {#    If so, convert it to a Python date #}
    {% set run_at_date_input = var('run_at_date', False) %}
    
    {% if run_at_date_input %}
        {% set run_at_date = python_date(run_at_date_input) %}
    
    {# 3. Get a default date in the timezone of your DB. #}
    {% else %}

        {% set utc_timestamp = run_started_at.astimezone(utc) %} 
        {% set local_timestamp = utc_timestamp.astimezone(los_angeles) %}

        {#  Actual current datetime without microsecond. #}
        {% set run_at_date = local_timestamp.today().replace(microsecond=0) %}

        {# Yesterday's date is common in ETL/ELT for nightly runs. #}
        {# {%set run_at_date = (local_timestamp.date() - time_delta(days = 1))#} #} 
    {% endif %}

    {# 4. Return the run at date as a Python date object. #}
    {% do return(run_at_date) %}
{% endmacro %}