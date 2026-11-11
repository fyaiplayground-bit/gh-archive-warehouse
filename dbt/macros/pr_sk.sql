{#- Surrogate key for a pull request, built from its business key (base repo + number). -#}
{% macro pr_sk(repo_id_column, pr_number_column) -%}
    {{ surrogate_key([repo_id_column, pr_number_column]) }}
{%- endmacro %}
