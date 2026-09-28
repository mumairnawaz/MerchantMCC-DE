{#
    Overrides dbt's default schema-naming (which would concatenate the
    target schema and the custom schema, e.g. "main_marts"). We want clean,
    predictable schema names (staging / intermediate / marts) inside the
    same gold.duckdb file that src/gold/pipeline.py's "main" schema already
    lives in — this keeps dbt's own output visibly separate from the S14
    Python-owned Gold tables without polluting names.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
