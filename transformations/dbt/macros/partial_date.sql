{% macro parse_partial_date(col) -%}
-- Return timestamp from SDTM partial date strings (YYYY, YYYY-MM, YYYY-MM-DD)
case
  when {{ col }} rlike '^\\d{4}-\\d{2}-\\d{2}$' then to_timestamp_tz({{ col }})
  when {{ col }} rlike '^\\d{4}-\\d{2}-\\?\\?$' then to_timestamp_tz({{ col }} || '-15') -- midpoint of month
  when {{ col }} rlike '^\\d{4}-\\?\\? -\\?\\?$' then to_timestamp_tz({{ col }}[1:4] || '-07-01') -- July 1 as placeholder
  when {{ col }} rlike '^\\d{4}$' then to_timestamp_tz({{ col }} || '-07-01')
  else null
end
{%- endmacro %}
