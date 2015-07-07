/*
 * GeometryBean.java
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
package com.geodetix.geo.ejb;


import javax.ejb.*;

import java.util.*;

import org.postgis.*;

import com.geodetix.geo.value.GeometryValue;

/**
 * This BMP (Bean Managed Persistence) entity bean stores information
 * about geometrical description of a choosen entity in some application
 * domain. It stores geometrical infos in the <code>geometry</code> attribute
 * of type <code>org.postgis.Geometry</code>. Note that such an attribute could 
 * even be implemented as a pure text (i.e. Java <code>String</code>) field 
 * following the WKT format, thus leaving geometric details to the PostGIS 
 * api classes (see <code>README</code> file for further details).
 *
 * @ejb.bean
 *    name="Geometry"
 *    jndi-name="geodetix/geo/Geometry"
 *    local-jndi-name="geodetix/geo/GeometryLocal"
 *    type="BMP"
 *    view-type="local"
 *    primkey-field="id"
 *
 * @ejb.dao
 *     impl-class="com.geodetix.geo.dao.PostGisGeometryDAOImpl"
 *
 *
 * @ejb.transaction
 *     type="RequiresNew"
 *
 *
 * @ejb.value-object
 *     name="Geometry"
 *     match="value"
 *
 * @author  <a href="mailto:antonio.pace@geodetix.it">Antonio Pace</a>
 *
 */
public abstract class GeometryBean implements javax.ejb.EntityBean {
    
    
    /* TEMPORARY (NOT FOR REAL WORLD) PRIMARY KEY GENERATOR */
    private static final int SIMPLE_ID_GENERATOR = (int) System.currentTimeMillis();
    private static int actualId = SIMPLE_ID_GENERATOR;
    
    
    
    /* BMP fields */
    private Integer   id;
    private String    description;
    private Geometry  geometry;
    
    
    
    
    /* CREATE METHODS */
    
    
    /**
     * Creates a new Geometry Bean.
     * 
     * @ejb.create-method 
     *
     * @param geometry the <code>org.postgis.Geometry</code> that has to be 
     * included in this EJB.
     * @param description the textual description of this bean.
     * @throws CreateException lauched if an error occours during the 
     * EJB creation.
     * @return the EJB's primary key.
     */
    public Integer ejbCreate(org.postgis.Geometry geometry,String description)
    throws CreateException {
        
        this.id    = (new Integer(actualId++));
        this.geometry = geometry;
        this.description=description;
        
        /* For now return null, this will be overrided in the DAO implementation of this method */
        return null;
    }
    
    /**
     * This method is called by the container after the EJB creation.
     *
     * @param geometry the <code>org.postgis.Geometry</code> that has to be 
     * included in this EJB.
     */
    public void ejbPostCreate(org.postgis.Geometry geometry) {
        
        // Do something with Relationship.
    }
    
    
    
    
    
    /* ========== Finder Methods =========== */
    
    
    /**
     * Find a Geometry Bean from his primary key.
     * 
     * @param primaryKey the primary key of the bean to found.
     * @throws FinderException lauched if an error occours during the 
     * EJB search operation.
     */
    public abstract Integer ejbFindByPrimaryKey(Integer primaryKey)
    throws FinderException;
    
    
    
    
    /**
     * Find all of the geometry beans contained in a <code>Polygon</code>.
     *
     * @param polygon the Polygon to search in.
     */
    public abstract Collection ejbFindByPolygon(org.postgis.Polygon polygon)
    throws FinderException;
    
 
    
    
    
    /* ============== BMP Fields Accessor Methods ============ */
    
    
    /**
     * Returns the EJB's id field.
     *
     * @ejb.pk-field
     * @ejb.interface-method
     * @ejb.value-object match="value"
     */
    public Integer getId() {
        
        return this.id;
    }
    
    
    /**
     * Modifies the id field.
     */
    public void setId(Integer id) {
        
        this.id = id;
        
        this.makeDirty();
    }
    
    
    /**
     * Returns the EJB's textual description.
     *
     * @ejb.interface-method
     * @ejb.value-object match="value"
     * @return the Geometry Bean description.
     */
    public String getDescription() {
        
        return this.description;
    }
    
    /**
     * Modifies the EJB's textual description.
     *
     * @ejb.interface-method
     * @param description the Geometry Bean description.
     */
    public void setDescription(String description) {
        
        this.description = description;
        
        this.makeDirty();
    }
    
    
    
    /**
     * Returns the EJB's geometrical description.
     *
     * @ejb.interface-method
     * @ejb.value-object match="value"
     * @return the org.postgis.Geometry included in this bean.
     */
    public org.postgis.Geometry getGeometry() {
        
        return this.geometry;
    }
    
    /**
     * Modifies the EJB's geometrical description.
     *
     * @ejb.interface-method
     * @param geometry the <code>org.postgis.Geometry</code> that has to 
     * be included in this EJB.
     */
    public void setGeometry(org.postgis.Geometry geometry) {
        
        this.geometry = geometry;
        
        this.makeDirty();
    }
    
    
    
    
    
    /* HOME INTERFACE BMP UTILITY METHODS */
    
    
    /**
     * Create a non-OpenGIS DataBase table, used to persist the Geometry Beans.
     *
     * @ejb.home-method
     *
     * @dao.call
     */
    public abstract void ejbHomeMakeDbTable();
    
    
    
    /**
     * Create OpenGIS DataBase table, used to persist the Geometry Beans.
     *
     * @ejb.home-method
     *
     * @dao.call
     */
    public abstract void ejbHomeMakeDbTable(String gemetryType, int srid, int geometryDimension);
    
    
    
    /**
     * Remove Bean's Persistence Teable of the DataBase.
     *
     * @ejb.home-method
     *
     * @dao.call
     */
    public abstract void ejbHomeDropDbTable();
    
    
    
    /* VALUE OBJECTS */
    
    /**
     * This is an abstract method to allow XDoclet to expose
     * value object functionalities in the local/remote interface.
     *
     * @ejb.interface-method
     *
     * @return a value object for this GeometryBean.
     */
    public abstract GeometryValue getGeometryValue();
    
    
    
    
    
    /* XDOCLET BMP METHODS RELATED STUFF */
    
    
    /**
     * @see com.geodetix.geo.ejb.GeometryBMP source.
     */
    protected abstract void makeDirty();
    
}
