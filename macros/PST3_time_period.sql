{% macro last_day_of_month(any_day) %}
    {% set next_month = any_day.replace(day=28) + modules.datetime.timedelta(days=4) %}
    # subtracting the number of the current day brings us back one month
    {{ return (next_month - modules.datetime.timedelta(days=next_month.day)) }}
{% endmacro %}

{% macro period_calculate(time, selection_date="today", prefix="", suffix="") %}
    {%- set period = "" %}
    {%- set begin_date = "" %}
    {%- set end_date = "" %}
    {%- if time == "semesterly" %}
        {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() - modules.datetime.timedelta(days=6*30) %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}

        {%- if selection_date.month <= 6 %}
            {% set semester = "1" %}
        {%- else %}
            {% set semester = "2" %}
        {%- endif %}

        {%- if semester == "1" %}
            {% set begin_date = selection_date.replace(day=1, month=1).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace(day=30, month=6).strftime("%Y-%m-%d") %}
        {%- else %}
            {% set begin_date = selection_date.replace(day=1, month=7, year=selection_date.year).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace(day=31, month=12, year=selection_date.year).strftime("%Y-%m-%d") %}
        {%- endif %}
        {% set period = prefix + selection_date.year| string() + suffix + semester %}
    {%- endif %}

    {%- if time == "daily" %}
        {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}
        {% set begin_date = (selection_date - modules.datetime.timedelta(days=1)).strftime("%Y-%m-%d") %}
        {% set end_date = selection_date.strftime("%Y-%m-%d") %}
        {% set period = prefix + selection_date.year| string() + suffix %}
    {%- endif %}
    {%- if time == "monthly" %}
         {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}

        {% set begin_date = selection_date.replace(day=1) %}
        {% set end_date = last_day_of_month(selection_date) %}
        {% set period = prefix + begin_date.year| string() + suffix + (begin_date.strftime("%m")) %}
    {%- endif %}
    {%- if time == "quarterly" %}
        {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}

        {%- if selection_date.month <= 3 %}
            {% set begin_date = selection_date.replace( day=1, month=10, year=selection_date.year - 1).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace( day=31, month=12, year=selection_date.year - 1).strftime("%Y-%m-%d") %}
            {% set quarter = 4 %}
            {% set period = prefix + (selection_date.year - 1)| string() + suffix + quarter| string() %}
        {%- elif selection_date.month <= 6 %}
            {% set begin_date = selection_date.replace( day=1, month=1 ).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace( day=31, month=3).strftime("%Y-%m-%d") %}
            {% set quarter = 1 %}
            {% set period = prefix + (selection_date.year)| string() + suffix + quarter| string() %}
        {%- elif selection_date.month <= 9 %}
            {% set begin_date = selection_date.replace( day=1, month=4 ).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace( day=30, month=6).strftime("%Y-%m-%d") %}
            {% set quarter = 2 %}
            {% set period = prefix + (selection_date.year)| string() + suffix + quarter| string() %}
        {%- elif selection_date.month <= 12 %}
            {% set begin_date = selection_date.replace( day=1, month=7 ).strftime("%Y-%m-%d") %}
            {% set end_date = selection_date.replace( day=30, month=9).strftime("%Y-%m-%d") %}
            {% set quarter = 3 %}
            {% set period = prefix + (selection_date.year)| string() + suffix + quarter| string() %}
        {%- endif %}
    {%- endif %}
    {%- if time == "yearly" %}    
        {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}
        {% set begin_date = selection_date.replace( day=1, month=1, year=selection_date.year - 1 ).strftime("%Y-%m-%d") %}
        {% set end_date = selection_date.replace( day=31, month=12, year=selection_date.year - 1).strftime("%Y-%m-%d") %}
        {% set period = prefix + (selection_date.year -1 )| string() + suffix %}
    {%- endif %}
    {%- if time == "weekly" %}
        {%- if selection_date == "today" %}
            {% set selection_date = modules.datetime.datetime.now() %}
        {%- else %}
            {% set selection_date = modules.datetime.datetime.strptime(selection_date, "%Y-%m-%d").date() %}
        {%- endif %}
        {% set begin_date = selection_date - modules.datetime.timedelta(days=selection_date.weekday()) %}
        {% set end_date = begin_date + modules.datetime.timedelta(days=6) %}
        {% set period = prefix + (selection_date.year)| string() + suffix + begin_date.strftime("%W")| string()  %}
        {% set begin_date = begin_date.strftime("%Y-%m-%d") %}
        {% set end_date = end_date.strftime("%Y-%m-%d") %}
    {%- endif %}  
    {{ return({"period":period,"begin_date":begin_date,"end_date":end_date }) }}
{% endmacro %}
