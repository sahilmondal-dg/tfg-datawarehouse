{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {#- In prod, use the custom schema exactly as declared (gold / silver / bronze).
            This avoids the dbt default behaviour of prepending the target schema,
            which would produce <prod_target>_gold instead of gold. -#}
        {{ custom_schema_name | trim }}

    {%- else -%}

        {#- In all non-prod environments (dev, ci, etc.) use the custom schema exactly
            as declared, matching prod behaviour. -#}
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
