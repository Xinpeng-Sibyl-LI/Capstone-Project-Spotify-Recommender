-- macros/get_popularity_range.sql
-- Custom macro to categorize popularity scores into ranges
-- This demonstrates Jinja logic and can be reused across models

{% macro get_popularity_range(popularity_column) %}
    case 
        when {{ popularity_column }} >= 80 then 'Very High (80-100)'
        when {{ popularity_column }} >= 60 then 'High (60-79)'
        when {{ popularity_column }} >= 40 then 'Medium (40-59)'
        when {{ popularity_column }} >= 20 then 'Low (20-39)'
        when {{ popularity_column }} >= 0 then 'Very Low (0-19)'
        else 'Unknown'
    end
{% endmacro %}