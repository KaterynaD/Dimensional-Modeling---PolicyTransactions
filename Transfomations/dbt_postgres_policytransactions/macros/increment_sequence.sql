{%- macro increment_sequence() -%}
  
  nextval('{{ model.schema }}.{{ this.name }}_seq')

{%- endmacro -%}