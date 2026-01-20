{% macro trimmer(column_name,node) %}

    TRIM( {{ column_name | trim | upper }} )

{% endmacro %}