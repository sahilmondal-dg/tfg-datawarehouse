{% macro silver_prefix(source_name, table_name) %}
{#
  Selects all columns from a source table and renames each one
  with a "silver_" prefix.  Used by every silver model so the
  pattern stays in one place.
#}
{% set source_rel = source(source_name, table_name) %}
{% set cols = adapter.get_columns_in_relation(source_rel) %}

SELECT
{% for col in cols %}
    {{ col.quoted }} AS silver_{{ col.name }}{% if not loop.last %},{% endif %}

{% endfor %}
FROM {{ source_rel }}
{% endmacro %}
