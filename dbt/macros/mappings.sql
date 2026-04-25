{% macro map_event_type(col) %}
    case {{ col }}
        when 'EQ' then 'Earthquake'
        when 'FL' then 'Flood'
        when 'TC' then 'Tropical Cyclone'
        when 'DR' then 'Drought'
        when 'VO' then 'Volcano'
        when 'WF' then 'Wildfire'
        when 'TS' then 'Tsunami'
        else 'Other'
    end
{% endmacro %}

{% macro map_alert_level(col) %}
    case {{ col }}
        when 'GREEN'  then 1
        when 'ORANGE' then 2
        when 'RED'    then 3
        else 0
    end
{% endmacro %}

{% macro map_reliefweb_type(col) %}
    case lower({{ col }})
        when 'earthquake'         then 'EQ'
        when 'flood'              then 'FL'
        when 'tropical cyclone'   then 'TC'
        when 'drought'            then 'DR'
        when 'volcano'            then 'VO'
        when 'wild fire'          then 'WF'
        when 'wildfire'           then 'WF'
        when 'tsunami'            then 'TS'
        when 'storm'              then 'TC'
        else 'OT'
    end
{% endmacro %}