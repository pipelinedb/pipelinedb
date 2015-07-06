/*
 * PGShapeGeometry.java
 * 
 * Allows PostGIS data to be read directly into a java2d shape
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

package org.postgis.java2d;

import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.*;
import java.sql.SQLException;

import org.postgresql.util.PGobject;

/**
 * PostGIS Java2D geometry implementation (read-only).
 * 
 * Supports PostGIS 1.x (lwgeom hexwkb).
 * 
 * As the java.awt.Shape methods currently are implemented by using a
 * java.awt.geom.GeneralPath object, they have the same semantics.
 * 
 * BUG/TODO: MultiPoints or Points in a Geometry Collection currently don't work
 * as expected, as some GeneralPath implementations throw away adjacent MoveTo
 * commands as an optimization (e. G. sun 1.5 and ibm 1.5). Points thus are
 * translated into MoveTo() followed by a closePath. This may change when we
 * implement our own path logics. We have to evaluate whether Graphics2D renders
 * a single MoveTo command as a single "brush tip", or we need the closePath()
 * command nevertheless to get any drawing. Maybe we need a LineTo() to the same
 * coordinages instead.
 * 
 * (Multi)LineStrings are translated into a sequence of a single MoveTo and
 * multiple LineTo vertices, and Polygon rings into a sequence of a single
 * MoveTo, multiple LineTo and a closePath command. To allow correct Polygon
 * filling, our PathIterators have GeneralPath.WIND_EVEN_ODD as winding rule.
 * 
 * @see java.awt.geom.GeneralPath
 * @see java.awt.Shape
 * @see org.postgresql.util.PGobject
 * 
 * @author Markus Schaber
 */

public class PGShapeGeometry extends PGobject implements Shape {
    /* JDK 1.5 Serialization */
    private static final long serialVersionUID = 0x100;

    final static ShapeBinaryParser parser = new ShapeBinaryParser();

    private final GeneralPath path;

    private int srid;

    /**
     * Constructor called by JDBC drivers. call setValue afterwards to fill with
     * Geometry data.
     * 
     */
    public PGShapeGeometry() {
        setType("geometry");
        path = new GeneralPath(GeneralPath.WIND_EVEN_ODD);
    }

    /** Construct directly from a General Path */
    public PGShapeGeometry(GeneralPath path, int srid) {
        setType("geometry");
        this.path = path;
        this.srid = srid;
    }

    /** Reads the HexWKB representation */
    public PGShapeGeometry(String value) throws SQLException {
        this();
        setValue(value);
    }

    /**
     * Reads the HexWKB representation - to be called by the jdbc drivers. Be
     * shure to call this only once and if you used the PGShapeGeometry()
     * constructor without parameters. In all other cases, behaviour is
     * undefined.
     */
    public void setValue(String value) throws SQLException {
        srid = parser.parse(value, path);
    }

    public String toString() {
        return "PGShapeGeometry " + path.toString();
    }

    /** We currently have read-only support, so this method returns null */
    public String getValue() {
        return null;
    }

    public boolean equals(Object obj) {
        if (obj instanceof PGShapeGeometry)
            return ((PGShapeGeometry) obj).path.equals(path);
        return false;
    }

    /** Return the SRID or Geometry.UNKNOWN_SRID if none was available */
    public int getSRID() {
        return srid;
    }

    // following are the java2d Shape method implementations...
    public boolean contains(double x, double y) {
        return path.contains(x, y);
    }

    public boolean contains(double x, double y, double w, double h) {
        return path.contains(x, y, w, h);
    }

    public boolean intersects(double x, double y, double w, double h) {
        return path.intersects(x, y, w, h);
    }

    public Rectangle getBounds() {
        return path.getBounds();
    }

    public boolean contains(Point2D p) {
        return path.contains(p);
    }

    public Rectangle2D getBounds2D() {
        return path.getBounds2D();
    }

    public boolean contains(Rectangle2D r) {
        return path.contains(r);
    }

    public boolean intersects(Rectangle2D r) {
        return path.intersects(r);
    }

    public PathIterator getPathIterator(AffineTransform at) {
        return path.getPathIterator(at);
    }

    public PathIterator getPathIterator(AffineTransform at, double flatness) {
        return path.getPathIterator(at, flatness);
    }
}
