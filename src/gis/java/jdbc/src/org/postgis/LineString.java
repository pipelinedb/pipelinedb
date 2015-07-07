/*
 * LineString.java
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

public class LineString extends PointComposedGeom {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    double len = -1.;

    public LineString() {
        super(LINESTRING);
    }

    public LineString(Point[] points) {
        super(LINESTRING, points);
    }

    public LineString(String value) throws SQLException {
        super(LINESTRING, value);
    }

    public LineString(String value, boolean haveM) throws SQLException {
        super(LINESTRING, value, haveM);
    }

    public LineString reverse() {
        Point[] points = this.getPoints();
        int l = points.length;
        int i, j;
        Point[] p = new Point[l];
        for (i = 0, j = l - 1; i < l; i++, j--) {
            p[i] = points[j];
        }
        return new LineString(p);
    }

    public LineString concat(LineString other) {
        Point[] points = this.getPoints();
        Point[] opoints = other.getPoints();

        boolean cutPoint = this.getLastPoint() == null
                || this.getLastPoint().equals(other.getFirstPoint());
        int count = points.length + opoints.length - (cutPoint ? 1 : 0);
        Point[] p = new Point[count];

        // Maybe we should use System.arrayCopy here?
        int i, j;
        for (i = 0; i < points.length; i++) {
            p[i] = points[i];
        }
        if (!cutPoint) {
            p[i++] = other.getFirstPoint();
        }
        for (j = 1; j < opoints.length; j++, i++) {
            p[i] = opoints[j];
        }
        return new LineString(p);
    }

    public double length() {
        if (len < 0) {
            Point[] points = this.getPoints();
            if ((points == null) || (points.length < 2)) {
                len = 0;
            } else {
                double sum = 0;
                for (int i = 1; i < points.length; i++) {
                    sum += points[i - 1].distance(points[i]);
                }
                len = sum;
            }
        }
        return len;
    }
}
