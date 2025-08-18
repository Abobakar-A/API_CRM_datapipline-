{% macro get_customer_segment(total_spending) %}
    CASE
        WHEN {{ total_spending }} >= 500 THEN 'High-Value'
        WHEN {{ total_spending }} >= 100 AND {{ total_spending }} < 500 THEN 'Mid-Value'
        ELSE 'Low-Value'
    END
{% endmacro %}