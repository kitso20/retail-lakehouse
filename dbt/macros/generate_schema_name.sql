-- dbt's default macro prefixes custom schemas with the target schema
-- (a model with +schema: silver in target schema public becomes
-- public_silver). Our silver/gold schemas are created independently by
-- sql/schema.sql, so custom schema names must be used verbatim.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
