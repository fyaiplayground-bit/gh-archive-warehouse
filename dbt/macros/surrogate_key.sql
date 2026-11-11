{#- Surrogate key: md5 of the given columns cast to text and joined with '-'.
    The single definition every surrogate key in the project uses, so keys are built the same way. -#}
{% macro surrogate_key(columns) -%}
    to_hex(md5(to_utf8(concat(
        {%- for column in columns %}
        cast({{ column }} as varchar){% if not loop.last %}, '-',{% endif %}
        {%- endfor %}
    ))))
{%- endmacro %}
