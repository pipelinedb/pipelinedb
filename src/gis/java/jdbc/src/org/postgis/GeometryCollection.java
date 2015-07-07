/*
 * GeometryCollection.java
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

/**
 * Geometry Collection class WARNING: Currently only implements empty
 * collections
 * 
 * @author markus.schaber@logix-tt.com
 * 
 */

public class GeometryCollection extends ComposedGeom {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    public static final String GeoCollID = "GEOMETRYCOLLECTION";

    public GeometryCollection() {
        super(GEOMETRYCOLLECTION);
    }

    public GeometryCollection(Geometry[] geoms) {
        super(GEOMETRYCOLLECTION, geoms);
    }

    public GeometryCollection(String value) throws SQLException {
        this(value, false);
    }

    public GeometryCollection(String value, boolean haveM) throws SQLException {
        super(GEOMETRYCOLLECTION, value, haveM);
    }

    protected Geometry[] createSubGeomArray(int ngeoms) {
        return new Geometry[ngeoms];
    }

    protected Geometry createSubGeomInstance(String token, boolean haveM) throws SQLException {
        return PGgeometry.geomFromString(token, haveM);
    }

    protected void innerWKT(StringBuffer SB) {
        subgeoms[0].outerWKT(SB, true);
        for (int i = 1; i < subgeoms.length; i++) {
            SB.append(',');
            subgeoms[i].outerWKT(SB, true);
        }
    }

    public Geometry[] getGeometries() {
        return subgeoms;
    }
}
