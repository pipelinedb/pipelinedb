CREATE OR REPLACE FUNCTION utmzone(geometry) RETURNS integer AS
$BODY$
DECLARE
    geomgeog geometry;
    zone int;
    pref int;
BEGIN
    geomgeog:=ST_Transform($1,4326);
    IF (ST_Y(geomgeog))>0 THEN
        pref:=32600;
    ELSE
        pref:=32700;
    END IF;
    zone:=floor((ST_X(geomgeog)+180)/6)+1;
    RETURN zone+pref;
END;
$BODY$ LANGUAGE 'plpgsql' immutable;
