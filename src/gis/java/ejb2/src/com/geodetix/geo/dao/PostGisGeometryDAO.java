/*
 * PostGisGeometryDAO.java
 *
 * Using PostGIS Geometry Types with EJB2 - Proof of Concept
 *
 * Copyright 2006, Geodetix S.r.l. (http://www.geodetix.it)
 * and individual contributors as indicated by the @authors tag.
 *
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation, either version 2.1 of the License.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA or visit the web at
 * http://www.gnu.org.
 *
 */
package com.geodetix.geo.dao;


/**
 * This is the DAO (Data Access Object) interface customized for
 * the PostGIS database which extends the XDoclet's auto-generated
 * GeometryDao interface and contains all of the SQL query statements.
 *
 * @author <a href="mailto:antonio.pace@geodetix.it">Antonio Pace</a>
 *
 */
public interface PostGisGeometryDAO extends GeometryDao {
    
    /* ========== Constants Definition ============ */
    
    /** DataSource Lookup String */
    public static final String DATASOURCE_NAME = "java:/postgis-ejb2-ds";
    
    /* ========== SQL Queries Definition ============ */
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.ejb.GeometryBean#ejbCreate(org.postgis.Geometry, java.lang.String)}
     * is called.
     */
    public static final String EJB_CREATE_STATEMENT = 
            "INSERT INTO geometries (id,geometry,description) VALUES (?,?,?)";
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#findByPrimaryKey(java.lang.Integer)}
     * is called.
     */
    public static final String FIND_BY_PRIMARY_KEY_STATEMENT = 
            "SELECT id FROM geometries WHERE id = ?";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#findByPolygon(org.postgis.Polygon)}
     * is called.
     */
    public static final String FIND_BY_POLYGON_STATEMENT = 
            "SELECT id FROM geometries WHERE contains(?,geometry)";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#makeDbTable()}
     * is called.
     */
    public static final String HOME_CREATE_NON_OPENGIS_TABLE_STATEMENT = 
            "CREATE TABLE geometries (id INT PRIMARY KEY, description TEXT, geometry GEOMETRY)";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#makeDbTable(java.lang.String, int, int)}
     * is called for create initial table.
     */
    public static final String HOME_CREATE_OPENGIS_TABLE_STATEMENT = 
            "CREATE TABLE geometries (id INT PRIMARY KEY, description TEXT)";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#makeDbTable(java.lang.String, int, int)}
     * is called for adding geometry column.
     */
    public static final String ADD_OPEN_GIS_GEOMETRY_COLUMN_STATEMENT = 
            "SELECT AddGeometryColumn('','geometries','geometry',?,?,?)";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#dropDbTable()}
     * is called for adding geometry column.
     */
    public static final String DROP_TABLE_STATEMENT = 
            "DROP TABLE geometries";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.ejb.GeometryBMP#ejbLoad()}
     * is called from the container.
     */
    public static final String EJB_LOAD_STATEMENT = 
            "SELECT id,geometry,description FROM geometries WHERE id=?";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.ejb.GeometryBMP#ejbStore()}
     * is called from the container.
     */
    public static final String EJB_STORE_STATEMENT = 
            "UPDATE geometries SET geometry=?, description=? WHERE id=?";
    
    
    /**
     * Query executed from 
     * {@link com.geodetix.geo.dao.PostGisGeometryDAOImpl}
     * on PostGIS when the method 
     * {@link com.geodetix.geo.ejb.GeometryBean#ejbRemove()}
     * is called from the container.
     */
    public static final String EJB_REMOVE_STATEMENT = 
            "DELETE FROM geometries WHERE id = ?";
    
} // end PostGisGeometryDAO
