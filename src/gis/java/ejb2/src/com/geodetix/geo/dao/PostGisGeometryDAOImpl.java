/*
 * PostGisGeometryDAOImpl.java
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

import org.apache.commons.logging.*;

import org.postgis.*;

import java.sql.*;

import java.util.*;

import javax.ejb.*;

import javax.naming.*;

import javax.sql.*;

import java.io.*;

/**
 *
 * PostGis DAO Implementation for 
 * {@link com.geodetix.geo.ejb.GeometryBean} BMP Persistence.
 *
 * @author  <a href="mailto:antonio.pace@geodetix.it">antonio.pace</a>
 *
 */
public class PostGisGeometryDAOImpl implements PostGisGeometryDAO {
    
    
    private DataSource  dataSource;
    private Context     jndiCntx;
    
    /**
     * Creates a new instance of PostGisPointDAO.
     */
    public PostGisGeometryDAOImpl() {}
    
    
    
    /** Initializes the bean. */
    public void init() {
        try {
            
            jndiCntx   = new InitialContext();
            dataSource = (DataSource) jndiCntx.lookup(PostGisGeometryDAO.DATASOURCE_NAME);
            
        } catch (NamingException ne) {
            throw new EJBException(ne);
        }
    }
    
    
    
    
    /* =========== Create Methods ============ */
    
    
    
    /**
     * PostGIS implementation of the 
     * {@link com.geodetix.geo.ejb.GeometryBean#ejbCreate(org.postgis.Geometry, java.lang.String)}
     * method.
     * 
     * @return the primary key of the persisted bean.
     * @param ejb the bean to persist.
     * @throws javax.ejb.CreateException launched if an EJB creation error is encountered.
     * @throws javax.ejb.EJBException launched if a generic EJB error is encountered.
     */
    public java.lang.Integer create(com.geodetix.geo.ejb.GeometryBean ejb)
    throws javax.ejb.CreateException, javax.ejb.EJBException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.EJB_CREATE_STATEMENT);
            
            pstm.setInt(1, ejb.getId());
            pstm.setObject(2, new PGgeometry((Geometry)ejb.getGeometry()));
            pstm.setString(3,ejb.getDescription());
            
            if (pstm.executeUpdate() != 1) {
                throw new CreateException(
                        "Failed to add EJB to database");
            }
            
            return ejb.getId();
            
        } catch (SQLException se) {
            throw new EJBException(se);
            
        } finally {
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
                
            } catch (Exception e) {}
        }
    }
    
    
    
    
    
    
    /* ============ Finder Methods ============== */
    
    
    /**
     * PostGIS implementation of the 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#findByPrimaryKey(java.lang.Integer)}
     * method
     * 
     * 
     * @return the found bean's prymary key.
     * @param primaryKey primary key of searching bean.
     * @throws javax.ejb.FinderException launched if an error occours during the search operation.
     */
    public java.lang.Integer findByPrimaryKey(java.lang.Integer primaryKey)
    throws javax.ejb.FinderException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        ResultSet        result = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.FIND_BY_PRIMARY_KEY_STATEMENT);
            
            pstm.setInt(1, primaryKey.intValue());
            
            result = pstm.executeQuery();
            
            if (!result.next()) {
                throw new ObjectNotFoundException(
                        "Cannot find Geometry Bean with id = " + primaryKey);
            }
            
        } catch (SQLException se) {
            throw new EJBException(se);
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
            } catch (Exception e) {}
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {}
        }
        
        return primaryKey;
    }
    
    
    
    
    /**
     * PostGIS implementation of the  
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#findByPolygon(org.postgis.Polygon)}
     * method
     * 
     * @return a collection of bean's primary key beeing found.
     * @param polygon the {@link org.postgis.Polygon} to search in.
     * @throws javax.ejb.FinderException launched if an error occours during the search operation.
     */
    public java.util.Collection findByPolygon(org.postgis.Polygon polygon)
    throws javax.ejb.FinderException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        ResultSet        result = null;
        
        try {
            
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.FIND_BY_POLYGON_STATEMENT);
            
            pstm.setObject(1, new PGgeometry(polygon));
            
            result = pstm.executeQuery();
            
            Vector keys = new Vector();
            
            while (result.next()) {
                keys.addElement(result.getObject("id"));
            }
            
            return keys;
            
        } catch (SQLException se) {
            throw new EJBException(se);
            
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
            } catch (Exception e) {}
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {}
        }
    }
    
    
    
    
    /* =========== Bean's Life Cycle Methods ============= */
    
    
    
    /**
     * PostGIS implementation of the entity bean's life cycle method 
     * <code>ejbLoad()</code>.
     * 
     * @param pk the primary key of the bean to load.
     * @param ejb the ejb whose data must be loaded.
     * @throws javax.ejb.EJBException launched if a generic EJB error is encountered.
     */
    public void load(java.lang.Integer pk, com.geodetix.geo.ejb.GeometryBean ejb)
    throws javax.ejb.EJBException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        ResultSet        result = null;
        
        try {
            
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.EJB_LOAD_STATEMENT);
            
            pstm.setInt(1, pk.intValue());
            
            result = pstm.executeQuery();
            
            if (result.next()) {
                ejb.setId(pk);
                ejb.setGeometry(((PGgeometry) result.getObject("geometry")).getGeometry());
                ejb.setDescription((String) result.getString("description"));
                
            } else {
                
                throw new EJBException("ejbLoad unable to load EJB.");
            }
            
        } catch (SQLException se) {
            throw new EJBException(se);
            
        } finally {
            try {
                if (result != null) {
                    result.close();
                }
            } catch (Exception e) {}
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {}
        }
    }
    
    
    /**
     * PostGIS implementation of the entity bean's lifecicle method 
     * <code>ejbStore()</code>.
     * 
     * @param ejb the ejb whose data must be stored.
     * @throws javax.ejb.EJBException launched if a generic EJB error is encountered.
     */
    public void store(com.geodetix.geo.ejb.GeometryBean ejb)
    throws javax.ejb.EJBException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.EJB_STORE_STATEMENT);
            
            pstm.setObject(1, new PGgeometry(ejb.getGeometry()));
            pstm.setString(2,ejb.getDescription());
            pstm.setInt(3, ejb.getId().intValue());
            
            if (pstm.executeUpdate() != 1) {
                throw new EJBException("ejbStore unable to update EJB.");
            }
            
        } catch (SQLException se) {
            
            throw new EJBException(se);
            
        } finally {
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
                
            } catch (Exception e) {}
        }
    }
    
    
    /**
     * PostGIS implementation of the entity bean's lifecicle method 
     * <code>ejbRemove()</code>.
     * 
     * 
     * @param pk primary key of the bean that must be removed .
     * @throws javax.ejb.RemoveException launched if an error during
     *   EJB remove operation is encountered.
     * @throws javax.ejb.EJBException launched if a generic EJB error is encountered.
     */
    public void remove(java.lang.Integer pk)
    throws javax.ejb.RemoveException, javax.ejb.EJBException {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            pstm  = con.prepareStatement(PostGisGeometryDAO.EJB_REMOVE_STATEMENT);
            
            pstm.setInt(1, pk.intValue());
            
            if (pstm.executeUpdate() != 1) {
                throw new EJBException("ejbRemove unable to remove EJB.");
            }
        } catch (SQLException se) {
            
            throw new EJBException(se);
            
        } finally {
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
            } catch (Exception e) {}
        }
    }
    
    
    
    
    
    /* =========== Home Interface Utility Methods ============ */

    
    /**
     * PostGIS implementation of the 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#makeDbTable(java.lang.String, int, int)}
     * method creating a NON-OpenGis compliant table in the PostGIS database.
     * The table will contain the geometry EJBs.
     */
    public void makeDbTable() {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            System.out.println("Creating non-OpenGIG Bean table... ");
            
            pstm = con.prepareStatement(PostGisGeometryDAO.HOME_CREATE_NON_OPENGIS_TABLE_STATEMENT);
            pstm.execute();
            
            System.out.println("...done!");
            
        } catch (SQLException e) {
            throw new EJBException(e);
            
        } finally {
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
                
            } catch (Exception e) {}
        }
    }
    
    
    
    /**
     * PostGIS implementation of the 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#makeDbTable(java.lang.String, int, int)}
     * method creating an OpenGIS compliant table in the PostGIS database.
     * The table will contain the geometry EJBs.
     *
     * @param gemetryType the string that rapresent a valid PostGIS 
     * geometry type (i.e.: POINT, LINESTRING, POLYGON etc.).
     * @param srid the SRID of the geometry.
     * @param geometryDimension the dimension of the geometry.
     */
    public void makeDbTable(String gemetryType, int srid, int geometryDimension) {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            System.out.println("Creating OpenGIS Bean table...");
            
            pstm = con.prepareStatement(PostGisGeometryDAO.HOME_CREATE_OPENGIS_TABLE_STATEMENT);
            pstm.execute();
            
            pstm = con.prepareStatement(PostGisGeometryDAO.ADD_OPEN_GIS_GEOMETRY_COLUMN_STATEMENT);
            pstm.setInt(1,srid);
            pstm.setString(2,gemetryType);
            pstm.setInt(3,geometryDimension);
            
            pstm.execute();
            
            System.out.println("...done!");
            
        } catch (SQLException e) {
            throw new EJBException(e);
            
        } finally {
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
                
            } catch (Exception e) {}
        }
    }
    
    
    /**
     * PostGIS implementation of the 
     * {@link com.geodetix.geo.interfaces.GeometryLocalHome#dropDbTable()}
     * method removing the table related to the EJBs.
     */
    public void dropDbTable() {
        
        PreparedStatement  pstm = null;
        Connection          con = null;
        
        
        try {
            
            con = this.dataSource.getConnection();
            
            System.out.println("Dropping Bean Table... ");
            
            pstm = con.prepareStatement(PostGisGeometryDAO.DROP_TABLE_STATEMENT);
            pstm.execute();
            
            System.out.println("...done!");
            
        } catch (SQLException e) {
            
            throw new EJBException(e);
            
        } finally {
            
            try {
                if (pstm != null) {
                    pstm.close();
                }
            } catch (Exception e) {}
            
            try {
                if (con != null) {
                    con.close();
                }
                
            } catch (Exception e) {}
        }
    }
    
    
}