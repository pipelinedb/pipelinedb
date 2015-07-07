/*
 * GeometryFacadeBean.java
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


import com.geodetix.geo.exception.ApplicationGeoLayerException;
import com.geodetix.geo.interfaces.GeometryLocalHome;
import com.geodetix.geo.interfaces.GeometryLocal;
import com.geodetix.geo.util.GeometryUtil;
import com.geodetix.geo.value.GeometryValue;

import org.apache.commons.logging.*;

import org.postgis.*;

import java.rmi.*;

import java.util.*;

import javax.ejb.*;

import javax.naming.*;


/**
 * A SessionFacade for managing Geometry beans.
 *
 * @ejb.bean
 *     name="GeometryFacade"
 *     jndi-name="geodetix/geo/GeometryFacade"
 *     local-jndi-name="geodetix/geo/GeometryFacadeLocal"
 *     type="Stateless"
 *     view-type="both"
 *     transaction-type="Container"
 *
 * @ejb.ejb-ref
 *     ejb-name="Geometry"
 *     view-type="local"
 *     ref-name="ejb/GeometryLocal"
 *
 * @author  <a href="mailto:antonio.pace@geodetix.it">Antonio Pace</a>
 */
public abstract class GeometryFacadeBean implements SessionBean {
    private SessionContext ctx;
    
    
    /**
     * EJB Initializer.
     *
     * @ejb.create-method
     */
    public void ejbCreate() throws CreateException {}
    
    /**
     * Called after the EJB creation.
     */
    public void ejbPostCreate() throws CreateException {}
    
    /**
     * Save the EJB session context.
     */
    public void setSessionContext(SessionContext ctx)
    throws EJBException, RemoteException {
        this.ctx = ctx;
    }
    
    
    
  
    /* ============ Finder Methods ============== */
    
    /**
     * Find all of the geometry beans contained in a <code>Polygon</code>.
     *
     * @ejb.interface-method
     *
     * @param polygon The Polygon to search in.
     */
    public Collection findByPolygon(org.postgis.Polygon polygon)
    throws ApplicationGeoLayerException {
        try {
            GeometryLocalHome geometryHome = GeometryUtil.getLocalHome();
            Collection       geometries    = geometryHome.findByPolygon(polygon);
            
            List result = new ArrayList();
            
            for (Iterator iter = geometries.iterator(); iter.hasNext(); ) {
                GeometryLocal geometry = (GeometryLocal) iter.next();
                
                result.add(geometry.getGeometryValue());
            }
            
            return result;
            
        } catch (NamingException e) {
            throw new ApplicationGeoLayerException(e);
        } catch (FinderException e) {
            throw new ApplicationGeoLayerException(e);
        }
    }
    
    
    
    
    
    /* =============== Business Iinterfaces Methods ============= */
    
    
    /**
     * Creates a new Geometry Bean.
     *
     * @ejb.interface-method
     *
     * @param geometry the <code>org.postgis.Geometry</code> that has to be 
     * included in this EJB.
     * @param description the textual description of this bean.
     * @return a value object representing the created EJB.
     */
    public GeometryValue createGeometry(org.postgis.Geometry geometry, String description)
    throws ApplicationGeoLayerException {
        try {
            GeometryLocalHome geometryHome  = GeometryUtil.getLocalHome();
            GeometryLocal     geometryLocal =
                    geometryHome.create(geometry,description);
            
            return geometryLocal.getGeometryValue();
        } catch (NamingException e) {
            throw new ApplicationGeoLayerException(e);
        } catch (CreateException e) {
            throw new ApplicationGeoLayerException(e);
        }
    }
    
    
    
    
    
    /* ================== BMP Utility Methods ================= */
    
    
    
    /**
     * Create a non-OpenGIS DataBase table used to persist the Geometry Beans.
     * <em>Note that in a real-world application this method should be protected
     * by using a role-based security policy.</em>
     *
     * @ejb.interface-method
     *
     * @throws ApplicationGeoLayerException thrown if an error occours 
     * during table creation.
     */
    public void createGeometryTable() throws ApplicationGeoLayerException {
        try {
            GeometryLocalHome geometryHome = GeometryUtil.getLocalHome();
            
            geometryHome.makeDbTable();
        } catch (NamingException e) {
            throw new ApplicationGeoLayerException(e);
        } catch (Exception e) {
            throw new ApplicationGeoLayerException(e);
        }
    }
    
    
    /**
     * Create an OpenGIS compliant database table used to persist the 
     * Geometry Beans.
     * <em>Note that in a real-world application this method should be protected
     * by using a role-based security policy.</em>
     *
     * @ejb.interface-method
     *
     * @throws ApplicationGeoLayerException thrown if an error occours 
     * during table creation.
     */
    public void createGeometryTable(String gemetryType, int srid, int geometryDimension )
    throws ApplicationGeoLayerException {
        try {
            GeometryLocalHome geometryHome = GeometryUtil.getLocalHome();
            
            geometryHome.makeDbTable(gemetryType, srid,  geometryDimension);
            
        } catch (NamingException e) {
            throw new ApplicationGeoLayerException(e);
        } catch (Exception e) {
            throw new ApplicationGeoLayerException(e);
        }
    }
    
    
    
    /**
     * Remove the EJB's persistence table from the database.
     * <em>Note that in a real-world application this method should be protected
     * by using a role-based security policy.</em>
     *
     * @ejb.interface-method
     *
     * @throws ApplicationGeoLayerException thrown if an error occours 
     * during table creation.
     */
    public void dropGeometryTable() throws ApplicationGeoLayerException {
        try {
            GeometryLocalHome geometryHome = GeometryUtil.getLocalHome();
            
            geometryHome.dropDbTable();
        } catch (NamingException e) {
            throw new ApplicationGeoLayerException(e);
        } catch (Exception e) {
            throw new ApplicationGeoLayerException(e);
        }
    }
    
    
    
}
