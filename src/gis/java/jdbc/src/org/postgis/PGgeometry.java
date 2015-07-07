/*
 * PGgeometry.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - PGobject Geometry Wrapper
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

import org.postgis.binary.BinaryParser;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

public class PGgeometry extends PGobject {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    Geometry geom;
    BinaryParser bp = new BinaryParser();

    public PGgeometry() {
        this.setType("geometry");
    }

    public PGgeometry(Geometry geom) {
        this();
        this.geom = geom;
    }

    public PGgeometry(String value) throws SQLException {
        this();
        setValue(value);
    }

    public void setValue(String value) throws SQLException {
        geom = geomFromString(value, bp);
    }

    public static Geometry geomFromString(String value) throws SQLException {
        return geomFromString(value, false);
    }

    public static Geometry geomFromString(String value, boolean haveM) throws SQLException {
        BinaryParser bp = new BinaryParser();

        return geomFromString(value, bp, haveM);
    }

    /**
     * Maybe we could add more error checking here?
     */
    public static Geometry geomFromString(String value, BinaryParser bp) throws SQLException {
        return geomFromString(value, bp, false);
    }

    public static Geometry geomFromString(String value, BinaryParser bp, boolean haveM)
            throws SQLException {
        value = value.trim();

        int srid = Geometry.UNKNOWN_SRID;

        if (value.startsWith(SRIDPREFIX)) {
            // break up geometry into srid and wkt
            String[] parts = PGgeometry.splitSRID(value);
            value = parts[1].trim();
            srid = Geometry.parseSRID(Integer.parseInt(parts[0].substring(5)));
        }

        Geometry result;
        if (value.startsWith("00") || value.startsWith("01")) {
            result = bp.parse(value);
        } else if (value.endsWith("EMPTY")) {
            // We have a standard conforming representation for an empty
            // geometry which is to be parsed as an empty GeometryCollection.
            result = new GeometryCollection();
        } else if (value.startsWith("MULTIPOLYGON")) {
            result = new MultiPolygon(value, haveM);
        } else if (value.startsWith("MULTILINESTRING")) {
            result = new MultiLineString(value, haveM);
        } else if (value.startsWith("MULTIPOINT")) {
            result = new MultiPoint(value, haveM);
        } else if (value.startsWith("POLYGON")) {
            result = new Polygon(value, haveM);
        } else if (value.startsWith("LINESTRING")) {
            result = new LineString(value, haveM);
        } else if (value.startsWith("POINT")) {
            result = new Point(value, haveM);
        } else if (value.startsWith("GEOMETRYCOLLECTION")) {
            result = new GeometryCollection(value, haveM);
        } else {
            throw new SQLException("Unknown type: " + value);
        }

        if (srid != Geometry.UNKNOWN_SRID) {
            result.srid = srid;
        }

        return result;
    }

    public Geometry getGeometry() {
        return geom;
    }

    public void setGeometry(Geometry newgeom) {
        this.geom = newgeom;
    }
    
    public int getGeoType() {
        return geom.type;
    }

    public String toString() {
        return geom.toString();
    }

    public String getValue() {
        return geom.toString();
    }

    public Object clone() {
        return new PGgeometry(geom);
    }

    /** The prefix that indicates SRID presence */
    public static final String SRIDPREFIX = "SRID=";

    /**
     * Splits a String at the first occurrence of border charachter.
     * 
     * Poor man's String.split() replacement, as String.split() was invented at
     * jdk1.4, and the Debian PostGIS Maintainer had problems building the woody
     * backport of his package using DFSG-free compilers. In all the cases we
     * used split() in the org.postgis package, we only needed to split at the
     * first occurence, and thus this code could even be faster.
     * 
     * @throws SQLException
     */
    public static String[] splitSRID(String whole) throws SQLException {
        int index = whole.indexOf(';', 5); // sridprefix length is 5
        if (index == -1) {
            throw new SQLException("Error parsing Geometry - SRID not delimited with ';' ");
        } else {
            return new String[]{
                whole.substring(0, index),
                whole.substring(index + 1)};
        }
    }
}
