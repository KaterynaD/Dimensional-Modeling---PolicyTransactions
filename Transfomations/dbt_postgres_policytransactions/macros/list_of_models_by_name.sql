{% macro list_of_models_by_name(table_prefix) %}
    {% set tables = [] %}
    {% if execute %}
    {% for node_name, node in graph.nodes.items() %}
        {% if  node.name.startswith(table_prefix) %}
            {% do tables.append(node.name) %}
        {% endif %}
    {% endfor %}
    {% do return(tables) %}
    {% endif %}
{% endmacro %}