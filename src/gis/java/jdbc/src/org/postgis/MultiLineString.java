/*
 * MultiLineString.java
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

public class MultiLineString extends ComposedGeom {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    double len = -1;

    public int hashCode() {
        return super.hashCode() ^ (int) this.length();
    }

    public MultiLineString() {
        super(MULTILINESTRING);
    }

    public MultiLineString(LineString[] lines) {
        super(MULTILINESTRING, lines);
    }

    public MultiLineString(String value) throws SQLException {
        this(value, false);
    }

    public MultiLineString(String value, boolean haveM) throws SQLException {
        super(MULTILINESTRING, value, haveM);
    }

    protected Geometry createSubGeomInstance(String token, boolean haveM) throws SQLException {
        return new LineString(token, haveM);
    }

    protected Geometry[] createSubGeomArray(int nlines) {
        return new LineString[nlines];
    }

    public int numLines() {
        return subgeoms.length;
    }

    public LineString[] getLines() {
        return (LineString[]) subgeoms.clone();
    }

    public LineString getLine(int idx) {
        if (idx >= 0 & idx < subgeoms.length) {
            return (LineString) subgeoms[idx];
        } else {
            return null;
        }
    }

    public double length() {
        if (len < 0) {
            LineString[] lines = (LineString[]) subgeoms;
            if (lines.length < 1) {
                len = 0;
            } else {
                double sum = 0;
                for (int i = 0; i < lines.length; i++) {
                    sum += lines[i].length();
                }
                len = sum;
            }
        }
        return len;
    }
}
