/*
 * JtsBinaryParser.java
 * 
 * Binary Parser for JTS - relies on org.postgis V1.0.0+ package.
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
package org.postgis.jts;

import org.postgis.binary.ByteGetter;
import org.postgis.binary.ValueGetter;
import org.postgis.binary.ByteGetter.BinaryByteGetter;
import org.postgis.binary.ByteGetter.StringByteGetter;

import com.vividsolutions.jts.geom.*;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

/**
 * Parse binary representation of geometries. Currently, only text rep (hexed)
 * implementation is tested.
 * 
 * It should be easy to add char[] and CharSequence ByteGetter instances,
 * although the latter one is not compatible with older jdks.
 * 
 * I did not implement real unsigned 32-bit integers or emulate them with long,
 * as both java Arrays and Strings currently can have only 2^31-1 elements
 * (bytes), so we cannot even get or build Geometries with more than approx.
 * 2^28 coordinates (8 bytes each).
 * 
 * @author Markus Schaber, markus.schaber@logix-tt.com
 * 
 */
public class JtsBinaryParser {

    /**
     * Get the appropriate ValueGetter for my endianness
     * 
     * @param bytes
     *            The appropriate Byte Getter
     * 
     * @return the ValueGetter
     */
    public static ValueGetter valueGetterForEndian(ByteGetter bytes) {
        if (bytes.get(0) == ValueGetter.XDR.NUMBER) { // XDR
            return new ValueGetter.XDR(bytes);
        } else if (bytes.get(0) == ValueGetter.NDR.NUMBER) {
            return new ValueGetter.NDR(bytes);
        } else {
            throw new IllegalArgumentException("Unknown Endian type:" + bytes.get(0));
        }
    }

    /**
     * Parse a hex encoded geometry
     */
    public Geometry parse(String value) {
        StringByteGetter bytes = new ByteGetter.StringByteGetter(value);
        return parseGeometry(valueGetterForEndian(bytes));
    }

    /**
     * Parse a binary encoded geometry.
     */
    public Geometry parse(byte[] value) {
        BinaryByteGetter bytes = new ByteGetter.BinaryByteGetter(value);
        return parseGeometry(valueGetterForEndian(bytes));
    }

    /** Parse a geometry starting at offset. */
    protected Geometry parseGeometry(ValueGetter data) {
        return parseGeometry(data, 0, false);
    }

    /** Parse with a known geometry factory */
    protected Geometry parseGeometry(ValueGetter data, int srid, boolean inheritSrid) {
        byte endian = data.getByte(); // skip and test endian flag
        if (endian != data.endian) {
            throw new IllegalArgumentException("Endian inconsistency!");
        }
        int typeword = data.getInt();

        int realtype = typeword & 0x1FFFFFFF; // cut off high flag bits

        boolean haveZ = (typeword & 0x80000000) != 0;
        boolean haveM = (typeword & 0x40000000) != 0;
        boolean haveS = (typeword & 0x20000000) != 0;

        if (haveS) {
            int newsrid = org.postgis.Geometry.parseSRID(data.getInt());
            if (inheritSrid && newsrid != srid) {
                throw new IllegalArgumentException("Inconsistent srids in complex geometry: " + srid + ", " + newsrid);
            } else {
                srid = newsrid;
            }
        } else if (!inheritSrid) {
            srid = org.postgis.Geometry.UNKNOWN_SRID;
        }
       
        Geometry result;
        switch (realtype) {
        case org.postgis.Geometry.POINT:
            result = parsePoint(data, haveZ, haveM);
            break;
        case org.postgis.Geometry.LINESTRING:
            result = parseLineString(data, haveZ, haveM);
            break;
        case org.postgis.Geometry.POLYGON:
            result = parsePolygon(data, haveZ, haveM, srid);
            break;
        case org.postgis.Geometry.MULTIPOINT:
            result = parseMultiPoint(data, srid);
            break;
        case org.postgis.Geometry.MULTILINESTRING:
            result = parseMultiLineString(data, srid);
            break;
        case org.postgis.Geometry.MULTIPOLYGON:
            result = parseMultiPolygon(data, srid);
            break;
        case org.postgis.Geometry.GEOMETRYCOLLECTION:
            result = parseCollection(data, srid);
            break;
        default:
            throw new IllegalArgumentException("Unknown Geometry Type!");
        }
        
        result.setSRID(srid);
        
        return result;
    }

    private Point parsePoint(ValueGetter data, boolean haveZ, boolean haveM) {
        double X = data.getDouble();
        double Y = data.getDouble();
        Point result;
        if (haveZ) {
            double Z = data.getDouble();
            result = JtsGeometry.geofac.createPoint(new Coordinate(X, Y, Z));
        } else {
            result = JtsGeometry.geofac.createPoint(new Coordinate(X, Y));
        }

        if (haveM) { // skip M value
            data.getDouble();
        }
        
        return result;
    }

    /** Parse an Array of "full" Geometries */
    private void parseGeometryArray(ValueGetter data, Geometry[] container, int srid) {
        for (int i = 0; i < container.length; i++) {
            container[i] = parseGeometry(data, srid, true);
        }
    }

    /**
     * Parse an Array of "slim" Points (without endianness and type, part of
     * LinearRing and Linestring, but not MultiPoint!
     * 
     * @param haveZ
     * @param haveM
     */
    private CoordinateSequence parseCS(ValueGetter data, boolean haveZ, boolean haveM) {
        int count = data.getInt();
        int dims = haveZ ? 3 : 2;
        CoordinateSequence cs = new PackedCoordinateSequence.Double(count, dims);

        for (int i = 0; i < count; i++) {
            for (int d = 0; d < dims; d++) {
                cs.setOrdinate(i, d, data.getDouble());
            }
            if (haveM) { // skip M value
                data.getDouble();
            }
        }
        return cs;
    }

    private MultiPoint parseMultiPoint(ValueGetter data, int srid) {
        Point[] points = new Point[data.getInt()];
        parseGeometryArray(data, points, srid);
        return JtsGeometry.geofac.createMultiPoint(points);
    }

    private LineString parseLineString(ValueGetter data, boolean haveZ, boolean haveM) {
        return JtsGeometry.geofac.createLineString(parseCS(data, haveZ, haveM));
    }

    private LinearRing parseLinearRing(ValueGetter data, boolean haveZ, boolean haveM) {
        return JtsGeometry.geofac.createLinearRing(parseCS(data, haveZ, haveM));
    }

    private Polygon parsePolygon(ValueGetter data, boolean haveZ, boolean haveM, int srid) {
        int holecount = data.getInt() - 1;
        LinearRing[] rings = new LinearRing[holecount];
        LinearRing shell = parseLinearRing(data, haveZ, haveM);
        shell.setSRID(srid);
        for (int i = 0; i < holecount; i++) {
            rings[i] = parseLinearRing(data, haveZ, haveM);
            rings[i].setSRID(srid);
        }
        return JtsGeometry.geofac.createPolygon(shell, rings);
    }

    private MultiLineString parseMultiLineString(ValueGetter data, int srid) {
        int count = data.getInt();
        LineString[] strings = new LineString[count];
        parseGeometryArray(data, strings, srid);
        return JtsGeometry.geofac.createMultiLineString(strings);
    }

    private MultiPolygon parseMultiPolygon(ValueGetter data, int srid) {
        int count = data.getInt();
        Polygon[] polys = new Polygon[count];
        parseGeometryArray(data, polys, srid);
        return JtsGeometry.geofac.createMultiPolygon(polys);
    }

    private GeometryCollection parseCollection(ValueGetter data, int srid) {
        int count = data.getInt();
        Geometry[] geoms = new Geometry[count];
        parseGeometryArray(data, geoms, srid);
        return JtsGeometry.geofac.createGeometryCollection(geoms);
    }
}
