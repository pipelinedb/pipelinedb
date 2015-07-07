/*
 * ComposedGeom.java
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
 * PointComposedGeom - base class for all composed geoms that contain only
 * points.
 * 
 * @author markus.schaber@logix-tt.com
 * 
 */

public abstract class PointComposedGeom extends ComposedGeom {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    protected PointComposedGeom(int type) {
        super(type);
    }

    protected PointComposedGeom(int type, Point[] points) {
        super(type, points);
    }

    public PointComposedGeom(int type, String value) throws SQLException {
        this(type, value, false);
    }

    public PointComposedGeom(int type, String value, boolean haveM) throws SQLException {
        super(type, value, haveM);
    }

    protected Geometry createSubGeomInstance(String token, boolean haveM) throws SQLException {
        return new Point(token, haveM);
    }

    protected Geometry[] createSubGeomArray(int pointcount) {
        return new Point[pointcount];
    }

    protected void innerWKT(StringBuffer sb) {
        subgeoms[0].innerWKT(sb);
        for (int i = 1; i < subgeoms.length; i++) {
            sb.append(',');
            subgeoms[i].innerWKT(sb);
        }
    }

    /**
     * optimized version
     */
    public int numPoints() {
        return subgeoms.length;
    }

    /**
     * optimized version
     */
    public Point getPoint(int idx) {
        if (idx >= 0 & idx < subgeoms.length) {
            return (Point) subgeoms[idx];
        } else {
            return null;
        }
    }

    /** Get the underlying Point array */
    public Point[] getPoints() {
        return (Point[]) subgeoms;
    }
}
