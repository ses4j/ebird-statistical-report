-- expand this function with names of local birders

SET CLIENT_ENCODING TO 'UTF-8';
CREATE or REPLACE FUNCTION get_observer_name(varchar) RETURNS varchar AS
$$
select case $1
    WHEN 'obsr676032' THEN 'Scott Stafford'
    else concat('unknown (', $1, ')')
    end
$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
