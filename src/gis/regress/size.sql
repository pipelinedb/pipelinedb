WITH areal(g) AS  ( SELECT ST_Buffer('POINT(0 0)',1,1) ),
     lineal(g) AS ( SELECT ST_Boundary(g) FROM areal ),
     puntal(g) AS ( SELECT ST_StartPoint(g) FROM lineal ),
     alldim(g) AS ( SELECT g FROM areal UNION ALL
                    SELECT g FROM lineal UNION ALL
                    SELECT g FROM puntal ),
     alltyp(g) AS ( SELECT g FROM alldim UNION ALL
                    SELECT ST_Force3DM(g) FROM alldim UNION ALL
                    SELECT ST_Force3DZ(g) FROM alldim UNION ALL
                    SELECT ST_Force4D(g)  FROM alldim )
SELECT 'bbox',ST_Dimension(g) d, ST_ZMFlag(g) f,
 ST_MemSize(postgis_addbbox(g))-ST_MemSize(postgis_dropbbox(g))
FROM alltyp ORDER BY f,d;
