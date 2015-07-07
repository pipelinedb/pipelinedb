-- interpolate_from_address(local_address, from_address_l, to_address_l, from_address_r, to_address_r, local_road)
-- This function returns a point along the given geometry (must be linestring)
-- corresponding to the given address.  If the given address is not within
-- the address range of the road, null is returned.
-- This function requires that the address be grouped, such that the second and
-- third arguments are from one side of the street, while the fourth and
-- fifth are from the other.
-- in_side Side of street -- either 'L', 'R' or if blank ignores side of road
-- in_offset_m -- number of meters offset to the side
CREATE OR REPLACE FUNCTION interpolate_from_address(given_address INTEGER, in_addr1 VARCHAR, in_addr2 VARCHAR, in_road GEOMETRY, 
	in_side VARCHAR DEFAULT '',in_offset_m float DEFAULT 10) RETURNS GEOMETRY
AS $_$
DECLARE
  addrwidth INTEGER;
  part DOUBLE PRECISION;
  road GEOMETRY;
  result GEOMETRY;
  var_addr1 INTEGER; var_addr2 INTEGER;
  center_pt GEOMETRY; cl_pt GEOMETRY;
  npos integer;
  delx float; dely float;  x0 float; y0 float; x1 float; y1 float; az float;
  var_dist float; dir integer;
BEGIN
    IF in_road IS NULL THEN
        RETURN NULL;
    END IF;
    
	var_addr1 := to_number(in_addr1, '999999');
	var_addr2 := to_number(in_addr2, '999999');

    IF geometrytype(in_road) = 'LINESTRING' THEN
      road := ST_Transform(in_road, utmzone(ST_StartPoint(in_road)) );
    ELSIF geometrytype(in_road) = 'MULTILINESTRING' THEN
    	road := ST_GeometryN(in_road,1);
    	road := ST_Transform(road, utmzone(ST_StartPoint(road)) );
    ELSE
      RETURN NULL;
    END IF;

    addrwidth := greatest(var_addr1,var_addr2) - least(var_addr1,var_addr2);
    IF addrwidth = 0 or addrwidth IS NULL THEN
        addrwidth = 1;
    END IF;
    part := (given_address - least(var_addr1,var_addr2)) / trunc(addrwidth, 1);

    IF var_addr1 > var_addr2 THEN
        part := 1 - part;
    END IF;

    IF part < 0 OR part > 1 OR part IS NULL THEN
        part := 0.5;
    END IF;

    center_pt = ST_LineInterpolatePoint(road, part);
    IF in_side > '' AND in_offset_m > 0 THEN
    /** Compute point the point to the in_side of the geometry **/
    /**Take into consideration non-straight so we consider azimuth 
    	of the 2 points that straddle the center location**/ 
    	IF part = 0 THEN
    		az := ST_Azimuth (ST_StartPoint(road), ST_PointN(road,2));
    	ELSIF part = 1 THEN
    		az := ST_Azimuth (ST_PointN(road,ST_NPoints(road) - 1), ST_EndPoint(road));
    	ELSE 
    		/** Find the largest nth point position that is before the center point
    			This will be the start of our azimuth calc **/
    		SELECT i INTO npos
    			FROM generate_series(1,ST_NPoints(road)) As i 
    					WHERE part > ST_LineLocatePoint(road,ST_PointN(road,i)) 
    					ORDER BY i DESC;
    		IF npos < ST_NPoints(road) THEN				
    			az := ST_Azimuth (ST_PointN(road,npos), ST_PointN(road, npos + 1));
    		ELSE
    			az := ST_Azimuth (center_pt, ST_PointN(road, npos));
    		END IF;
    	END IF;
    	
        dir := CASE WHEN az < pi() THEN -1 ELSE 1 END;
        --dir := 1;
        var_dist := in_offset_m*CASE WHEN in_side = 'L' THEN -1 ELSE 1 END;
        delx := ABS(COS(az)) * var_dist * dir;
        dely := ABS(SIN(az)) * var_dist * dir;
        IF az > pi()/2 AND az < pi() OR az > 3 * pi()/2 THEN
			result := ST_Translate(center_pt, delx, dely) ;
		ELSE
			result := ST_Translate(center_pt, -delx, dely);
		END IF;
    ELSE
    	result := center_pt;
    END IF;
    result :=  ST_Transform(result, ST_SRID(in_road));
    --RAISE NOTICE 'start: %, center: %, new: %, side: %, offset: %, az: %', ST_AsText(ST_Transform(ST_StartPoint(road),ST_SRID(in_road))), ST_AsText(ST_Transform(center_pt,ST_SRID(in_road))),ST_AsText(result), in_side, in_offset_m, az;
    RETURN result;
END;
$_$ LANGUAGE plpgsql IMMUTABLE COST 10;
-- needed to ban stupid warning about how we are using deprecated functions 
-- yada yada yada need this to work in 2.0 too bah
ALTER FUNCTION interpolate_from_address(integer, character varying, character varying, geometry, character varying, double precision)
  SET client_min_messages='ERROR';
