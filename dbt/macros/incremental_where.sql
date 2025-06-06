{% macro incremental_where(timestamp_column, lookback_hours=24) %}
  {% if is_incremental() %}
    {{ timestamp_column }} > (
      select coalesce(
        max({{ timestamp_column }}) - interval '{{ lookback_hours }} hours',
        '1900-01-01'::timestamp
      )
      from {{ this }}
    )
  {% else %}
    1=1
  {% endif %}
{% endmacro %}