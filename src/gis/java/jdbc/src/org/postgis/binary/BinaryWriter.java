/*
 * BinaryWriter.java
 * 
 * PostGIS extension for PostgreSQL JDBC driver - Binary Writer
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
package org.postgis.binary;

import org.postgis.Geometry;
import org.postgis.GeometryCollection;
import org.postgis.LineString;
import org.postgis.LinearRing;
import org.postgis.MultiLineString;
import org.postgis.MultiPoint;
import org.postgis.MultiPolygon;
import org.postgis.Point;
import org.postgis.Polygon;

/**
 * Create binary representation of geometries. Currently, only text rep (hexed)
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
 * @author markus.schaber@logi-track.com
 * 
 */
public class BinaryWriter {

    /**
     * Get the appropriate ValueGetter for my endianness
     * 
     * @param bytes The appropriate Byte Getter
     * 
     * @return the ValueGetter
     */
    public static ValueSetter valueSetterForEndian(ByteSetter bytes, byte endian) {
        if (endian == ValueSetter.XDR.NUMBER) { // XDR
            return new ValueSetter.XDR(bytes);
        } else if (endian == ValueSetter.NDR.NUMBER) {
            return new ValueSetter.NDR(bytes);
        } else {
            throw new IllegalArgumentException("Unknown Endian type:" + endian);
        }
    }

    /**
     * Write a hex encoded geometry
     * 
     * Is synchronized to protect offset counter. (Unfortunately, Java does not
     * have neither call by reference nor multiple return values.) This is a
     * TODO item.
     * 
     * The geometry you put in must be consistent, geom.checkConsistency() must
     * return true. If not, the result may be invalid WKB.
     * 
     * @see Geometry#checkConsistency() the consistency checker
     */
    public synchronized String writeHexed(Geometry geom, byte REP) {
        int length = estimateBytes(geom);
        ByteSetter.StringByteSetter bytes = new ByteSetter.StringByteSetter(length);
        writeGeometry(geom, valueSetterForEndian(bytes, REP));
        return bytes.result();
    }

    public synchronized String writeHexed(Geometry geom) {
        return writeHexed(geom, ValueSetter.NDR.NUMBER);
    }

    /**
     * Write a binary encoded geometry.
     * 
     * Is synchronized to protect offset counter. (Unfortunately, Java does not
     * have neither call by reference nor multiple return values.) This is a
     * TODO item.
     * 
     * The geometry you put in must be consistent, geom.checkConsistency() must
     * return true. If not, the result may be invalid WKB.
     * 
     * @see Geometry#checkConsistency()
     */
    public synchronized byte[] writeBinary(Geometry geom, byte REP) {
        int length = estimateBytes(geom);
        ByteSetter.BinaryByteSetter bytes = new ByteSetter.BinaryByteSetter(length);
        writeGeometry(geom, valueSetterForEndian(bytes, REP));
        return bytes.result();
    }

    public synchronized byte[] writeBinary(Geometry geom) {
        return writeBinary(geom, ValueSetter.NDR.NUMBER);
    }

    /** Parse a geometry starting at offset. */
    protected void writeGeometry(Geometry geom, ValueSetter dest) {
        // write endian flag
        dest.setByte(dest.endian);

        // write typeword
        int typeword = geom.type;
        if (geom.dimension == 3) {
            typeword |= 0x80000000;
        }
        if (geom.haveMeasure) {
            typeword |= 0x40000000;
        }
        if (geom.srid != Geometry.UNKNOWN_SRID) {
            typeword |= 0x20000000;
        }

        dest.setInt(typeword);

        if (geom.srid != Geometry.UNKNOWN_SRID) {
            dest.setInt(geom.srid);
        }

        switch (geom.type) {
        case Geometry.POINT :
            writePoint((Point) geom, dest);
            break;
        case Geometry.LINESTRING :
            writeLineString((LineString) geom, dest);
            break;
        case Geometry.POLYGON :
            writePolygon((Polygon) geom, dest);
            break;
        case Geometry.MULTIPOINT :
            writeMultiPoint((MultiPoint) geom, dest);
            break;
        case Geometry.MULTILINESTRING :
            writeMultiLineString((MultiLineString) geom, dest);
            break;
        case Geometry.MULTIPOLYGON :
            writeMultiPolygon((MultiPolygon) geom, dest);
            break;
        case Geometry.GEOMETRYCOLLECTION :
            writeCollection((GeometryCollection) geom, dest);
            break;
        default :
            throw new IllegalArgumentException("Unknown Geometry Type: " + geom.type);
        }
    }

    /**
     * Writes a "slim" Point (without endiannes, srid ant type, only the
     * ordinates and measure. Used by writeGeometry as ell as writePointArray.
     */
    private void writePoint(Point geom, ValueSetter dest) {
        dest.setDouble(geom.x);
        dest.setDouble(geom.y);

        if (geom.dimension == 3) {
            dest.setDouble(geom.z);
        }

        if (geom.haveMeasure) {
            dest.setDouble(geom.m);
        }
    }

    /** Write an Array of "full" Geometries */
    private void writeGeometryArray(Geometry[] container, ValueSetter dest) {
        for (int i = 0; i < container.length; i++) {
            writeGeometry(container[i], dest);
        }
    }

    /**
     * Write an Array of "slim" Points (without endianness, srid and type, part
     * of LinearRing and Linestring, but not MultiPoint!
     */
    private void writePointArray(Point[] geom, ValueSetter dest) {
        // number of points
        dest.setInt(geom.length);
        for (int i = 0; i < geom.length; i++) {
            writePoint(geom[i], dest);
        }
    }

    private void writeMultiPoint(MultiPoint geom, ValueSetter dest) {
        dest.setInt(geom.numPoints());
        writeGeometryArray(geom.getPoints(), dest);
    }

    private void writeLineString(LineString geom, ValueSetter dest) {
        writePointArray(geom.getPoints(), dest);
    }

    private void writeLinearRing(LinearRing geom, ValueSetter dest) {
        writePointArray(geom.getPoints(), dest);
    }

    private void writePolygon(Polygon geom, ValueSetter dest) {
        dest.setInt(geom.numRings());
        for (int i = 0; i < geom.numRings(); i++) {
            writeLinearRing(geom.getRing(i), dest);
        }
    }

    private void writeMultiLineString(MultiLineString geom, ValueSetter dest) {
        dest.setInt(geom.numLines());
        writeGeometryArray(geom.getLines(), dest);
    }

    private void writeMultiPolygon(MultiPolygon geom, ValueSetter dest) {
        dest.setInt(geom.numPolygons());
        writeGeometryArray(geom.getPolygons(), dest);
    }

    private void writeCollection(GeometryCollection geom, ValueSetter dest) {
        dest.setInt(geom.numGeoms());
        writeGeometryArray(geom.getGeometries(), dest);
    }

    /** Estimate how much bytes a geometry will need in WKB. */
    protected int estimateBytes(Geometry geom) {
        int result = 0;

        // write endian flag
        result += 1;

        // write typeword
        result += 4;

        if (geom.srid != Geometry.UNKNOWN_SRID) {
            result += 4;
        }

        switch (geom.type) {
        case Geometry.POINT :
            result += estimatePoint((Point) geom);
            break;
        case Geometry.LINESTRING :
            result += estimateLineString((LineString) geom);
            break;
        case Geometry.POLYGON :
            result += estimatePolygon((Polygon) geom);
            break;
        case Geometry.MULTIPOINT :
            result += estimateMultiPoint((MultiPoint) geom);
            break;
        case Geometry.MULTILINESTRING :
            result += estimateMultiLineString((MultiLineString) geom);
            break;
        case Geometry.MULTIPOLYGON :
            result += estimateMultiPolygon((MultiPolygon) geom);
            break;
        case Geometry.GEOMETRYCOLLECTION :
            result += estimateCollection((GeometryCollection) geom);
            break;
        default :
            throw new IllegalArgumentException("Unknown Geometry Type: " + geom.type);
        }
        return result;
    }

    private int estimatePoint(Point geom) {
        // x, y both have 8 bytes
        int result = 16;
        if (geom.dimension == 3) {
            result += 8;
        }

        if (geom.haveMeasure) {
            result += 8;
        }
        return result;
    }

    /** Write an Array of "full" Geometries */
    private int estimateGeometryArray(Geometry[] container) {
        int result = 0;
        for (int i = 0; i < container.length; i++) {
            result += estimateBytes(container[i]);
        }
        return result;
    }

    /**
     * Write an Array of "slim" Points (without endianness and type, part of
     * LinearRing and Linestring, but not MultiPoint!
     */
    private int estimatePointArray(Point[] geom) {
        // number of points
        int result = 4;

        // And the amount of the points itsself, in consistent geometries
        // all points have equal size.
        if (geom.length > 0) {
            result += geom.length * estimatePoint(geom[0]);
        }
        return result;
    }

    private int estimateMultiPoint(MultiPoint geom) {
        // int size
        int result = 4;
        if (geom.numPoints() > 0) {
            // We can shortcut here, as all subgeoms have the same fixed size
            result += geom.numPoints() * estimateBytes(geom.getFirstPoint());
        }
        return result;
    }

    private int estimateLineString(LineString geom) {
        return estimatePointArray(geom.getPoints());
    }

    private int estimateLinearRing(LinearRing geom) {
        return estimatePointArray(geom.getPoints());
    }

    private int estimatePolygon(Polygon geom) {
        // int length
        int result = 4;
        for (int i = 0; i < geom.numRings(); i++) {
            result += estimateLinearRing(geom.getRing(i));
        }
        return result;
    }

    private int estimateMultiLineString(MultiLineString geom) {
        // 4-byte count + subgeometries
        return 4 + estimateGeometryArray(geom.getLines());
    }

    private int estimateMultiPolygon(MultiPolygon geom) {
        // 4-byte count + subgeometries
        return 4 + estimateGeometryArray(geom.getPolygons());
    }

    private int estimateCollection(GeometryCollection geom) {
        // 4-byte count + subgeometries
        return 4 + estimateGeometryArray(geom.getGeometries());
    }
}
