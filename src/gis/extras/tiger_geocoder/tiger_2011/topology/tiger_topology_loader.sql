/*******************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * Copyright 2011 Leo Hsu and Regina Obe <lr@pcorp.us> 
 * Paragon Corporation
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU General Public Licence. See the COPYING file.
 *
 * This file contains helper functions for loading tiger data
 * into postgis topology structure
 **********************************************************************/

 /** topology_load_tiger: Will load all edges, faces, nodes into 
 *  topology named toponame
 *	that intersect the specified region
 *  region_type: 'place', 'county'
 *  region_id: the respective fully qualified geoid
 *	 place - plcidfp
 *	 county - cntyidfp 
 * USE CASE: 
 *  The following will create a topology called topo_boston 
 *   in Mass State Plane feet and load Boston, MA tiger data
 *  with tolerance of 1 foot
 * SELECT topology.DropTopology('topo_boston');
 * SELECT topology.CreateTopology('topo_boston', 2249,0.25);
 * SELECT tiger.topology_load_tiger('topo_boston', 'place', '2507000'); 
 * SELECT topology.TopologySummary('topo_boston');
 * SELECT topology.ValidateTopology('topo_boston');  
 ****/
CREATE OR REPLACE FUNCTION tiger.topology_load_tiger(IN toponame varchar,  
	region_type varchar, region_id varchar)
  RETURNS text AS
$$
DECLARE
 	var_sql text;
 	var_rgeom geometry;
 	var_statefp text;
 	var_rcnt bigint;
 	var_result text := '';
 	var_srid int := 4269;
 	var_precision double precision := 0;
BEGIN
	CASE region_type
		WHEN 'place' THEN
			SELECT the_geom , statefp FROM place INTO var_rgeom, var_statefp WHERE plcidfp = region_id;
		WHEN 'county' THEN
			SELECT the_geom, statefp FROM county INTO var_rgeom, var_statefp WHERE cntyidfp = region_id;
		ELSE
			RAISE EXCEPTION 'Region type % IS NOT SUPPORTED', region_type;
	END CASE;
	SELECT srid, precision FROM topology.topology into var_srid, var_precision
                WHERE name = toponame;
	var_sql := '
	CREATE TEMPORARY TABLE tmp_edge
   				AS 
	WITH te AS 
   			(SELECT tlid,  ST_GeometryN(ST_SnapToGrid(ST_Transform(ST_LineMerge(the_geom),$3),$4),1) As geom, tnidf, tnidt, tfidl, tfidr , the_geom As orig_geom
									FROM tiger.edges 
									WHERE statefp = $1 AND ST_Covers($2, the_geom)
										)
					SELECT DISTINCT ON (t.tlid) t.tlid As edge_id,t.geom 
                        , t.tnidf As start_node, t.tnidt As end_node, COALESCE(t.tfidl,0) As left_face
                        , COALESCE(t.tfidr,0) As right_face, COALESCE(tl.tlid, t.tlid) AS next_left_edge,  COALESCE(tr.tlid, t.tlid) As next_right_edge, t.orig_geom
						FROM 
							te AS t LEFT JOIN te As tl ON (t.tnidf = tl.tnidt AND t.tfidl = tl.tfidl)
							 LEFT JOIN te As tr ON (t.tnidt = tr.tnidf AND t.tfidr = tr.tfidr)				
						';
	EXECUTE var_sql USING var_statefp, var_rgeom, var_srid, var_precision;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_rcnt::text || ' edges holding in temporary. ';
	var_sql := 'ALTER TABLE tmp_edge ADD CONSTRAINT pk_tmp_edge PRIMARY KEY(edge_id );';
	EXECUTE var_sql;
	-- CREATE node indexes on temporary edges
	var_sql := 'CREATE INDEX idx_tmp_edge_start_node ON tmp_edge USING btree (start_node ); CREATE INDEX idx_tmp_edge_end_node ON tmp_edge USING btree (end_node );';

	EXECUTE var_sql;

	-- CREATE face indexes on temporary edges
	var_sql := 'CREATE INDEX idx_tmp_edge_left_face ON tmp_edge USING btree (left_face ); CREATE INDEX idx_tmp_edge_right_face ON tmp_edge USING btree (right_face );';

	EXECUTE var_sql;

	-- CREATE edge indexes on temporary edges
	var_sql := 'CREATE INDEX idx_tmp_edge_next_left_edge ON tmp_edge USING btree (next_left_edge ); CREATE INDEX idx_tmp_edge_next_right_edge ON tmp_edge USING btree (next_right_edge);';

	EXECUTE var_sql;
	
	-- start load in faces
	var_sql := 'INSERT INTO ' || quote_ident(toponame) || '.face(face_id, mbr) 
						SELECT f.tfid, ST_Envelope(ST_Transform(f.the_geom,$3)) As mbr 
							FROM tiger.faces AS f
								WHERE statefp = $1 AND 
								(  tfid IN(SELECT left_face FROM tmp_edge)
									OR tfid IN(SELECT right_face FROM tmp_edge) OR ST_Covers($2, the_geom) )
							AND tfid NOT IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face) ';
	EXECUTE var_sql USING var_statefp, var_rgeom, var_srid;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_result || var_rcnt::text || ' faces added. ';
   -- end load in faces
   
   -- add remaining missing edges of present faces --
   var_sql := 'INSERT INTO tmp_edge(edge_id, geom, start_node, end_node, left_face, right_face, next_left_edge, next_right_edge, orig_geom)	
   			WITH te AS 
   			(SELECT tlid,  ST_GeometryN(ST_SnapToGrid(ST_Transform(ST_LineMerge(the_geom),$2),$3),1) As geom, tnidf, tnidt, tfidl, tfidr, the_geom As orig_geom 
									FROM tiger.edges 
									WHERE statefp = $1 AND
									 (tfidl IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face)
				OR tfidr IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face) )
				AND tlid NOT IN(SELECT edge_id FROM tmp_edge)
				 )
				
			SELECT DISTINCT ON (t.tlid) t.tlid As edge_id,t.geom 
                        , t.tnidf As start_node, t.tnidt As end_node, t.tfidl As left_face
                        , t.tfidr As right_face, tl.tlid AS next_left_edge,  tr.tlid As next_right_edge, t.orig_geom
				FROM 
						te AS t LEFT JOIN te As tl 
								ON (t.tnidf = tl.tnidt AND t.tfidl = tl.tfidl)
			LEFT JOIN te As tr ON (t.tnidt = tr.tnidf AND t.tfidr = tr.tfidr)
			';
	EXECUTE var_sql USING var_statefp, var_srid, var_precision;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_result || var_rcnt::text || ' edges of faces added. ';
   	-- start load in nodes
	var_sql := 'INSERT INTO ' || quote_ident(toponame) || '.node(node_id, geom)
					SELECT DISTINCT ON(tnid) tnid, geom
						FROM 
						( 
							SELECT start_node AS tnid, ST_StartPoint(e.geom) As geom 
								FROM tmp_edge As e LEFT JOIN ' || quote_ident(toponame) || '.node AS n ON e.start_node = n.node_id
						UNION ALL 
							SELECT end_node AS tnid, ST_EndPoint(e.geom) As geom 
							FROM tmp_edge As e LEFT JOIN ' || quote_ident(toponame) || '.node AS n ON e.end_node = n.node_id 
							WHERE n.node_id IS NULL) As f 
							WHERE tnid NOT IN(SELECT node_id FROM  ' || quote_ident(toponame) || '.node)
					 ';
	EXECUTE var_sql USING var_statefp, var_rgeom;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_result || ' ' || var_rcnt::text || ' nodes added. ';

   -- end load in nodes
   -- start Mark which nodes are contained in faces
   	var_sql := 'UPDATE ' || quote_ident(toponame) || '.node AS n
					SET containing_face = f.tfid
						FROM (SELECT tfid, the_geom
							FROM tiger.faces WHERE statefp = $1 
							AND tfid IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face) 
							) As f
						WHERE ST_ContainsProperly(f.the_geom, ST_Transform(n.geom,4269)) ';
	EXECUTE var_sql USING var_statefp, var_rgeom;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_result || ' ' || var_rcnt::text || ' nodes contained in a face. ';
   -- end Mark nodes contained in faces

   -- Set orphan left right to itself and set edges with missing faces to world face
   var_sql := 'UPDATE tmp_edge SET next_left_edge = -1*edge_id WHERE next_left_edge IS NULL OR next_left_edge NOT IN(SELECT edge_id FROM tmp_edge);
        UPDATE tmp_edge SET next_right_edge = -1*edge_id WHERE next_right_edge IS NULL OR next_right_edge NOT IN(SELECT edge_id FROM tmp_edge);
        UPDATE tmp_edge SET left_face = 0 WHERE left_face NOT IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face);
        UPDATE tmp_edge SET right_face = 0 WHERE right_face NOT IN(SELECT face_id FROM ' || quote_ident(toponame) || '.face);';
   EXECUTE var_sql;

   -- force edges start and end points to match the start and end nodes --
   var_sql := 'UPDATE tmp_edge SET geom = ST_SetPoint(ST_SetPoint(tmp_edge.geom, 0, s.geom), ST_NPoints(tmp_edge.geom) - 1,e.geom)  
                FROM ' || quote_ident(toponame) || '.node AS s, ' || quote_ident(toponame) || '.node As e
                WHERE s.node_id = tmp_edge.start_node AND e.node_id = tmp_edge.end_node AND 
                    ( NOT ST_Equals(s.geom, ST_StartPoint(tmp_edge.geom) ) OR NOT ST_Equals(e.geom, ST_EndPoint(tmp_edge.geom) ) ) '  ;   
    EXECUTE var_sql;
    GET DIAGNOSTICS var_rcnt = ROW_COUNT;
    var_result := var_result || ' ' || var_rcnt::text || ' edge start end corrected. ';
   -- TODO: Load in edges --
   var_sql := '
   	INSERT INTO ' || quote_ident(toponame) || '.edge(edge_id, geom, start_node, end_node, left_face, right_face, next_left_edge, next_right_edge)
					SELECT t.edge_id, t.geom, t.start_node, t.end_node, COALESCE(t.left_face,0) As left_face, COALESCE(t.right_face,0) As right_face, t.next_left_edge, t.next_right_edge
						FROM 
							tmp_edge AS t
							WHERE t.edge_id NOT IN(SELECT edge_id FROM ' || quote_ident(toponame) || '.edge) 				
						';
	EXECUTE var_sql USING var_statefp, var_rgeom;
	GET DIAGNOSTICS var_rcnt = ROW_COUNT;
	var_result := var_result || ' ' || var_rcnt::text || ' edges added. ';
	var_sql = 'DROP TABLE tmp_edge;';
	EXECUTE var_sql;
	RETURN var_result;
END
$$
  LANGUAGE plpgsql VOLATILE
  COST 1000;