-- Override dbt's default schema naming behavior
-- By default, dbt creates schemas like "default_custom" (e.g., staging_staging)
-- This macro tells dbt: if a custom schema is specified, use it as-is
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

-- Macro to create schemas if they don't exist
{% macro execute_create_schemas() %}
    {% set sql %}
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS marts;
    {% endset %}
    
    {% if execute %}
        {% do run_query(sql) %}
        {% do log("Schemas created successfully", info=true) %}
    {% endif %}
{% endmacro %}
