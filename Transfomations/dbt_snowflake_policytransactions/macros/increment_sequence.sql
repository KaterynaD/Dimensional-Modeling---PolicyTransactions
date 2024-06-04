{%- macro increment_sequence() -%}
  
{{ model.schema }}.{{ this.name }}_seq.NEXTVAL

{%- endmacro -%}