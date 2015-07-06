/*
 * ApplicationGeoLayerException.java
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
package com.geodetix.geo.exception;


/**
 * Generic Application Exception.
 *
 * @author <a href="mailto:antonio.pace@geodetix.it">Antonio Pace</a>
 *
 */
public class ApplicationGeoLayerException extends Exception {
    
    /**
     * Creates a new ApplicationGeoLayerException object.
     */
    public ApplicationGeoLayerException() {
        super("unknown");
        
    } // end ApplicationGeoLayerException()
    
    
    /**
     * Creates a new ApplicationGeoLayerException object.
     *
     * @param e the wrapped Exception.
     */
    public ApplicationGeoLayerException(Exception e) {
        super(e.getMessage());
        
    } // end ApplicationGeoLayerException()
    
    
    /**
     * Creates a new ApplicationGeoLayerException object.
     *
     * @param msg the wrapped Message.
     */
    public ApplicationGeoLayerException(String msg) {
        super(msg);
        
    } // end ApplicationGeoLayerException()
    
} // end ApplicationGeoLayerException
