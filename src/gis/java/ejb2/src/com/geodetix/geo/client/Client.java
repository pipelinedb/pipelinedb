/*
 * Client.java
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
package com.geodetix.geo.client;

import com.geodetix.geo.exception.ApplicationGeoLayerException;
import com.geodetix.geo.interfaces.GeometryFacade;
import com.geodetix.geo.interfaces.GeometryFacadeHome;

import com.geodetix.geo.util.GeometryFacadeUtil;
import com.geodetix.geo.value.GeometryValue;

import java.rmi.RemoteException;

import javax.naming.InitialContext;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.rmi.PortableRemoteObject;

import java.util.*;
import org.postgis.*;


/**
 * A client executing a simple Test Case and illustrating the Geometry Bean Usage.
 * 
 * @author  <a href="mailto:antonio.pace@geodetix.it">Antonio Pace</a>
 */
public class Client {
    
    
    private GeometryFacade geometryFacade;
    
    private Point p1;
    private Point p2;
    private LineString fromP1toP2;
    private Polygon searchPolygon;
    
    
    /**
     * Initializer method.
     *
     * @param geometryFacade the geometry facade object used to interact
     * with the EJBs
     */
    public Client(GeometryFacade geometryFacade) {
        
        this.geometryFacade=geometryFacade;
        
        this.init();
    }
    
    private void init(){
        
        // Create geometry stuff
        
        // City Stadium 
        this.p1= new Point(16.23034006,39.31054320);
        
        // City Train Station
        this.p2= new Point(16.26002601,39.31920668);
        
        this.fromP1toP2= new LineString(new Point[] {p1,p2});
        
        
        // Create search polygon
  
        Point[] points = new Point[]{
            
            new Point(16.16399297, 39.40109388),
            new Point(16.32368776, 39.39596998),
            new Point(16.32397242, 39.25335486),
            new Point(16.16399297, 39.25534748),
            new Point(16.16399297, 39.40109388),
        };
        
        LinearRing[] linearRings   = { new LinearRing(points) };
       
        // City Area Extension
        this.searchPolygon = new Polygon(linearRings);
    }
    
    
    
    private void executeNonOpenGisTest() throws ApplicationGeoLayerException, RemoteException {
        
        System.out.println("Execute some Tests on NON-OpenGIS Geometry EJBs");
        
        geometryFacade.createGeometryTable();
        
        System.out.println("create some geometry stuff...");
        
        this.showGeometry(p1);
        GeometryValue gv1 = geometryFacade.createGeometry(p1,"[ City Stadium ]");
        
        this.showGeometry(p2);
        GeometryValue gv2 = geometryFacade.createGeometry(p2,"[ City Train Station ]");
        
        this.showGeometry(fromP1toP2);
        GeometryValue gv3 = geometryFacade.createGeometry(fromP1toP2,"Line from "
                                                                     + gv1.getDescription()
                                                                     + " to " 
                                                                     + gv2.getDescription());
        
        System.out.println("Searching created geometries in City Area Perimeter: ");
        this.showGeometry(searchPolygon);
        
        Collection<GeometryValue>  findResults = (Collection<GeometryValue>) geometryFacade.findByPolygon(searchPolygon);
        
        System.out.println("Search Results:");
        
        for (GeometryValue geometry  : findResults ) {
            
            this.showGeometry(geometry);
        }
        
        geometryFacade.dropGeometryTable();
    }
    
    
    
    
    
    private void executeOpenGisTest() throws ApplicationGeoLayerException, RemoteException {
        
        System.out.println("Execute some Tests on OpenGIS Geometry EJBs");
        
        geometryFacade.createGeometryTable("POINT",4326,2);
        
        System.out.println("Create some Points ....");
        
        // Setting SRID
        this.p1.setSrid(4326);
        this.p2.setSrid(4326);
        this.searchPolygon.setSrid(4326);
        
        this.showGeometry(p1);
        GeometryValue gv1 = geometryFacade.createGeometry(p1,"[ City Stadium ]");
        
        this.showGeometry(p2);
        GeometryValue gv2 = geometryFacade.createGeometry(p2,"[ City Train Station ]");
        
        System.out.println();
        System.out.println("Searching created Points in City Area Perimeter: ");
        this.showGeometry(searchPolygon);
        
        // Note the use of geometricFacade finder method
        Collection<GeometryValue>  findResults = (Collection<GeometryValue>) geometryFacade.findByPolygon(searchPolygon);
        
        System.out.println("Search Results:");
        
        for (GeometryValue geometry  : findResults ) {
            
            this.showGeometry(geometry);
        }
        
        geometryFacade.dropGeometryTable();
    }
    
    
    
    private void showGeometry(Geometry geometry) {
        
        System.out.println("Geometry: "+geometry.getTypeString()+geometry.getValue() + " SRID: "+geometry.getSrid());
    }
    
    
    private void showGeometry(GeometryValue geometryValue) {
        
        System.out.println("EJB Id: "+geometryValue.getId());
        System.out.println("Description: "+geometryValue.getDescription());
        
        this.showGeometry(geometryValue.getGeometry());
    }
    
    
    /**
     * Main client method.
     *
     * @param args arguments from the command line
     */
    public static void main(String [] args) {
        
        try {
            
            GeometryFacadeHome geometryFacadeHome= GeometryFacadeUtil.getHome();
            
            GeometryFacade geometryFacade= geometryFacadeHome.create();
            
            Client client = new Client(geometryFacade);
            
            System.out.println("===============================");
            System.out.println("== TEST 1 =====================");
            System.out.println("===============================");
            client.executeNonOpenGisTest();
            
            System.out.println("===============================");
            System.out.println("== TEST 2 =====================");
            System.out.println("===============================");
            client.executeOpenGisTest();
            
            System.out.println("===============================");
            System.out.println("DONE.");
            
        } catch (ApplicationGeoLayerException ae) {
            ae.printStackTrace();
        } catch (java.rmi.RemoteException re) {
            re.printStackTrace();
        } catch (javax.naming.NamingException ne) {
            ne.printStackTrace();
        } catch (javax.ejb.CreateException ce) {
            ce.printStackTrace();
        }
        
    }
    
    
    
    private static Context getInitialContext()
    throws javax.naming.NamingException {
        
        return new javax.naming.InitialContext();
    }
    
}
