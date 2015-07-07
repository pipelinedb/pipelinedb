/*
 * Point.java
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

import org.postgresql.util.PGtokenizer;

import java.sql.SQLException;

public class Point extends Geometry {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    public static final boolean CUTINTS = true;

    public int hashCode() {
        return super.hashCode() ^ hashCode(x) ^ hashCode(y) ^ hashCode(z) ^ hashCode(m);
    }

    public static int hashCode(double value) {
        long v = Double.doubleToLongBits(value);
        return (int) (v ^ (v >>> 32));
    }

    protected boolean equalsintern(Geometry otherg) {
        Point other = (Point) otherg;
        return equals(other);
    }

	public static boolean double_equals(double a, double b) {
		if ( Double.isNaN(a) && Double.isNaN(b) ) {
			return true;
		}
		else {
			return (a == b);
		}
	}

    public final boolean equals(Point other) {
        boolean xequals = double_equals(x, other.x);
        boolean yequals = double_equals(y, other.y);
        boolean zequals = ((dimension == 2) || double_equals(z, other.z));
        boolean mequals = ((haveMeasure == false) || double_equals(m,other.m));
        boolean result = xequals && yequals && zequals && mequals;
        return result;
    }

    public Point getPoint(int index) {
        if (index == 0) {
            return this;
        } else {
            throw new ArrayIndexOutOfBoundsException("Point only has a single Point! " + index);
        }
    }

    /** Optimized versions for this special case */
    public Point getFirstPoint() {
        return this;
    }

    /** Optimized versions for this special case */
    public Point getLastPoint() {
        return this;
    }

    public int numPoints() {
        return 1;
    }

    /**
     * The X coordinate of the point.
     * In most long/lat systems, this is the longitude.
     */
    public double x;

    /**
     * The Y coordinate of the point.
     * In most long/lat systems, this is the latitude.
     */
    public double y;

    /**
     * The Z coordinate of the point.
     * In most long/lat systems, this is a radius from the 
     * center of the earth, or the height / elevation over
     * the ground.
     */
    public double z;

    /**
     * The measure of the point.
     */
    public double m = 0.0;

    public Point() {
        super(POINT);
    }

    /** Constructs a new Point
     * @param x the longitude / x ordinate
     * @param y the latitude / y ordinate
     * @param z the radius / height / elevation / z ordinate
     */
    public Point(double x, double y, double z) {
        this();
        this.x = x;
        this.y = y;
        this.z = z;
        dimension = 3;
    }

    /** Constructs a new Point
     * @param x the longitude / x ordinate
     * @param y the latitude / y ordinate
     */
    public Point(double x, double y) {
        this();
        this.x = x;
        this.y = y;
        this.z = 0.0;
        dimension = 2;
    }

    /**
     * Construct a Point from EWKT.
     * 
     * (3D and measures are legal, but SRID is not allowed).
     */
    public Point(String value) throws SQLException {
        this(value, false);
    }

    /**
     * Construct a Point
     * 
     * @param value The text representation of this point
     * @param haveM Hint whether we have a measure. This is used by other
     *            geometries parsing inner points where we only get "1 2 3 4"
     *            like strings without the "POINT(" and ")" stuff. If there
     *            acutally is a POINTM prefix, this overrides the given value.
     *            However, POINT does not set it to false, as they can be
     *            contained in measured collections, as in
     *            "GEOMETRYCOLLECTIONM(POINT(0 0 0))".
     */
    protected Point(String value, boolean haveM) throws SQLException {
        this();
        value = initSRID(value);

        if (value.indexOf("POINTM") == 0) {
            haveM = true;
            value = value.substring(6).trim();
        } else if (value.indexOf("POINT") == 0) {
            value = value.substring(5).trim();
        }
        PGtokenizer t = new PGtokenizer(PGtokenizer.removePara(value), ' ');
        try {
            x = Double.valueOf(t.getToken(0)).doubleValue();
            y = Double.valueOf(t.getToken(1)).doubleValue();
            haveM |= t.getSize() == 4;
            if ((t.getSize() == 3 && !haveM) || (t.getSize() == 4)) {
                z = Double.valueOf(t.getToken(2)).doubleValue();
                dimension = 3;
            } else {
                dimension = 2;
            }
            if (haveM) {
                m = Double.valueOf(t.getToken(dimension)).doubleValue();
            }
        } catch (NumberFormatException e) {
            throw new SQLException("Error parsing Point: " + e.toString());
        }
        haveMeasure = haveM;
    }

    public void innerWKT(StringBuffer sb) {
        sb.append(x);
        if (CUTINTS)
            cutint(sb);
        sb.append(' ');
        sb.append(y);
        if (CUTINTS)
            cutint(sb);
        if (dimension == 3) {
            sb.append(' ');
            sb.append(z);
            if (CUTINTS)
                cutint(sb);
        }
        if (haveMeasure) {
            sb.append(' ');
            sb.append(m);
            if (CUTINTS)
                cutint(sb);
        }
    }

    private static void cutint(StringBuffer sb) {
        int l = sb.length() - 2;
        if ((sb.charAt(l + 1) == '0') && (sb.charAt(l) == '.')) {
            sb.setLength(l);
        }
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public double getZ() {
        return z;
    }

    public double getM() {
        return m;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public void setZ(double z) {
        this.z = z;
    }

    public void setM(double m) {
        haveMeasure = true;
        this.m = m;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

    public void setZ(int z) {
        this.z = z;
    }

    public double distance(Point other) {
        double tx, ty, tz;
        if (this.dimension != other.dimension) {
            throw new IllegalArgumentException("Points have different dimensions!");
        }
        tx = this.x - other.x;
        switch (this.dimension) {
        case 1 :
            return Math.sqrt(tx * tx);
        case 2 :
            ty = this.y - other.y;
            return Math.sqrt(tx * tx + ty * ty);
        case 3 :
            ty = this.y - other.y;
            tz = this.z - other.z;
            return Math.sqrt(tx * tx + ty * ty + tz * tz);
        default :
            throw new IllegalArgumentException("Illegal dimension of Point" + this.dimension);
        }
    }

    public boolean checkConsistency() {
        return super.checkConsistency() && (this.dimension == 3 || this.z == 0.0)
                && (this.haveMeasure || this.m == 0.0);
    }
}
