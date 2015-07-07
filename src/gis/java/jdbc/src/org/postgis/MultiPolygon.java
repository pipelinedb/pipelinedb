/*
 * MultiPolygon.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - geometry model
 * 
 * (C) 2004 Paul Ramsey, pramsey@refractions.net
 * 
 * (C) 2005 Markus Schaber, markus.schaber@logix-tt.com
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

package org.postgis;

import java.sql.SQLException;

public class MultiPolygon extends ComposedGeom {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    public MultiPolygon() {
        super(MULTIPOLYGON);
    }

    public MultiPolygon(Polygon[] polygons) {
        super(MULTIPOLYGON, polygons);
    }

    public MultiPolygon(String value) throws SQLException {
        this(value, false);
    }

    protected MultiPolygon(String value, boolean haveM) throws SQLException {
        super(MULTIPOLYGON, value, haveM);
    }

    protected Geometry[] createSubGeomArray(int npolygons) {
        return new Polygon[npolygons];
    }

    protected Geometry createSubGeomInstance(String token, boolean haveM) throws SQLException {
        return new Polygon(token, haveM);
    }

    public int numPolygons() {
        return subgeoms.length;
    }

    public Polygon getPolygon(int idx) {
        if (idx >= 0 & idx < subgeoms.length) {
            return (Polygon) subgeoms[idx];
        } else {
            return null;
        }
    }
    
    public Polygon[] getPolygons() {
        return (Polygon[]) subgeoms;
    }
}
